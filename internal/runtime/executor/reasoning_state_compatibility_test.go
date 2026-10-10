package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func reasoningTestContext(key string) context.Context {
	return context.WithValue(context.Background(), util.ContextKeyAPIKey, key)
}

func reasoningTestConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{AuthDir: filepath.Join(t.TempDir(), "auth")}
}

func TestReasoningProvenancePreservesNativeAndUnknownStateAndToolPairs(t *testing.T) {
	ctx := reasoningTestContext("provenance-test-client")
	cfg := reasoningTestConfig(t)
	r := reasoningOrigins(cfg)
	scope := reasoningOriginScope(ctx)
	r.remember(scope, "grok-state", "xai")
	r.remember(scope, "gpt-state", "codex")
	body := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":"continue"},{"type":"reasoning","id":"foreign","encrypted_content":"grok-state"},{"type":"reasoning","id":"native","encrypted_content":"gpt-state"},{"type":"reasoning","id":"unknown","encrypted_content":"unknown-state"},{"type":"function_call","call_id":"call_1","name":"read","arguments":"{\"path\":\"x\"}"},{"type":"function_call_output","call_id":"call_1","output":"result"}]}`)
	got := stripForeignReasoningState(ctx, cfg, "codex", body)
	input := gjson.GetBytes(got, "input").Array()
	if len(input) != 5 || input[1].Get("id").String() != "native" || input[2].Get("id").String() != "unknown" {
		t.Fatalf("only known foreign state should be removed: %s", got)
	}
	if input[3].Get("call_id").String() != "call_1" || input[4].Get("call_id").String() != "call_1" || input[4].Get("output").String() != "result" {
		t.Fatalf("tool history changed: %s", got)
	}
	otherClient := stripForeignReasoningState(reasoningTestContext("other-client"), cfg, "codex", body)
	if !bytes.Equal(otherClient, body) {
		t.Fatal("provenance must not cross client identity boundaries")
	}
	if !bytes.Equal(stripForeignReasoningState(ctx, cfg, "codex", got), got) {
		t.Fatal("compatible state should be preserved without rewriting the body")
	}
}

func TestReasoningProvenanceRecordsUpstreamAndSurvivesRestart(t *testing.T) {
	ctx := reasoningTestContext("restart-test-client")
	cfg := reasoningTestConfig(t)
	chunk := []byte(`data: {"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"opaque-grok-state"}}`)
	observeReasoningOrigins(ctx, cfg, "xai", chunk)
	r := reasoningOrigins(cfg)
	reloaded := newReasoningOriginRegistry(r.path)
	if got := reloaded.issuer(reasoningOriginScope(ctx), "opaque-grok-state"); got != "xai" {
		t.Fatalf("persisted issuer = %q, want xai", got)
	}
	stored, err := os.ReadFile(r.path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte("opaque-grok-state")) || bytes.Contains(stored, []byte("restart-test-client")) {
		t.Fatal("registry persisted ciphertext or credentials")
	}
	key := reasoningOriginKey(reasoningOriginScope(ctx), "opaque-grok-state")
	entry := reloaded.entries[key]
	entry.At = time.Now().Add(-reasoningOriginTTL - time.Minute)
	reloaded.entries[key] = entry
	if reloaded.issuer(reasoningOriginScope(ctx), "opaque-grok-state") != "" {
		t.Fatal("expired state must not be classified as foreign")
	}
}

func TestCodexEncryptedReasoningRecoveryHTTPAndSSE(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			body := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":"continue"},{"type":"reasoning","encrypted_content":"legacy-state"},{"type":"function_call","call_id":"c1","name":"read","arguments":"{}"},{"type":"function_call_output","call_id":"c1","output":"done"}]}`)
			transport := xaiStreamRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				payload, _ := io.ReadAll(request.Body)
				if calls == 1 {
					if !bytes.Equal(payload, body) {
						t.Fatal("valid/unrecognised state was modified before upstream validation")
					}
					errorBody := `{"error":{"code":"invalid_encrypted_content","type":"invalid_request_error","message":"cannot decrypt"}}`
					if status == http.StatusOK {
						errorBody = "event: error\ndata: " + errorBody + "\n\n"
					}
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(errorBody)), Request: request}, nil
				}
				input := gjson.GetBytes(payload, "input").Array()
				if len(input) != 3 || input[1].Get("call_id").String() != "c1" || input[2].Get("call_id").String() != "c1" {
					t.Fatalf("recovery did not preserve visible/tool history: %s", payload)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n")), Request: request}, nil
			})
			ctx := context.Background()
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://test.invalid/responses", bytes.NewReader(body))
			response, repaired, err := doCodexRequestWithReasoningRecovery(ctx, &http.Client{Transport: transport}, request, body, UpstreamRecorder{})
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if calls != 2 || bytes.Equal(repaired, body) || response.Header.Get("X-Clirelay-Reasoning-Repaired") != "1" {
				t.Fatalf("recovery calls=%d, body=%s, headers=%v", calls, repaired, response.Header)
			}
		})
	}
}

func TestCodexReasoningRecoveryNeverReplaysPartialOutputOrLoops(t *testing.T) {
	body := []byte(`{"input":[{"type":"message","role":"user","content":"continue"},{"type":"reasoning","encrypted_content":"legacy"}]}`)
	for _, partial := range []bool{false, true} {
		calls := 0
		transport := xaiStreamRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			data := "data: {\"type\":\"error\",\"error\":{\"code\":\"invalid_encrypted_content\"}}\n\n"
			if partial {
				data = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" + data
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data)), Request: r}, nil
		})
		request, _ := http.NewRequest(http.MethodPost, "https://test.invalid/responses", bytes.NewReader(body))
		response, _, err := doCodexRequestWithReasoningRecovery(context.Background(), &http.Client{Transport: transport}, request, body, UpstreamRecorder{})
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		want := 2
		if partial {
			want = 1
		}
		if calls != want {
			t.Fatalf("partial=%v calls=%d want=%d", partial, calls, want)
		}
	}
}

func TestCodexPrefetchPreservesLargeFramesAndReadErrors(t *testing.T) {
	data := "data: {\"type\":\"response.created\",\"padding\":\"" + strings.Repeat("x", 40000) + "\"}\n\n"
	reset := errors.New("stream error: INTERNAL_ERROR; received from peer")
	response := &http.Response{StatusCode: http.StatusOK, Body: &replayResponseBody{Reader: io.MultiReader(strings.NewReader(data), &terminalReadError{err: reset}), closer: io.NopCloser(strings.NewReader(""))}}
	if got := peekCodexEncryptedError(response); got != nil {
		t.Fatalf("unexpected error: %s", got)
	}
	got, err := io.ReadAll(response.Body)
	response.Body.Close()
	if string(got) != data || !errors.Is(err, reset) {
		t.Fatalf("prefetch changed the stream: bytes=%d err=%v", len(got), err)
	}
}

func TestXAIStreamCancellationDoesNotBlockProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, util.ContextKeyRoundTripper, xaiStreamRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n")), Request: r}, nil
	}))
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": "https://test.invalid", "api_key": "test"}}
	result, err := NewXAIExecutor(&config.Config{}).ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "grok-4.7", Payload: []byte(`{"model":"grok-4.7","stream":true,"input":"hi"}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: true})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	done := make(chan struct{})
	go func() {
		for range result.Chunks {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled stream leaked its producer")
	}
}

func TestUnpublishedStreamContentIsReleased(t *testing.T) {
	reporter := &usageReporter{captureFullContent: true}
	reporter.appendOutputChunk(bytes.Repeat([]byte("x"), usageReporterOutputMemoryLimit+1))
	path := reporter.outputPath
	if path == "" {
		t.Fatal("test did not create a spool file")
	}
	reporter.discardUnpublishedContent()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("canceled spool file remains: %v", err)
	}
}

func TestCodexExecutorRecoversLegacyReasoningWithoutClientChanges(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "nonstream"
		if streaming {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			key := t.Name()
			calls := 0
			transport := xaiStreamRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				body, _ := io.ReadAll(r.Body)
				status := http.StatusOK
				data := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-ok\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1,\"total_tokens\":6}}}\n\n"
				if calls == 1 {
					if !bytes.Contains(body, []byte("legacy-state")) {
						t.Fatal("unknown state was prematurely removed")
					}
					status = http.StatusBadRequest
					data = `{"error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":"cannot decrypt"}}`
					if streaming {
						status = http.StatusOK
						data = "event: error\ndata: " + data + "\n\n"
					}
				} else if bytes.Contains(body, []byte("legacy-state")) {
					t.Fatal("legacy state remains in recovered request")
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data)), Request: r}, nil
			})
			ctx := context.WithValue(reasoningTestContext(key), util.ContextKeyRoundTripper, transport)
			plugin := &tenantUsageCapturePlugin{records: make(chan coreusage.Record, 8)}
			coreusage.RegisterPlugin(plugin)
			auth := &cliproxyauth.Auth{ID: "compatibility-auth", Provider: "codex", Attributes: map[string]string{"base_url": "https://test.invalid", "api_key": "test-token"}}
			req := cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":"continue"},{"type":"reasoning","encrypted_content":"legacy-state"}]}`)}
			opts := cliproxyexecutor.Options{Stream: streaming, SourceFormat: sdktranslator.FromString("openai-response")}
			executor := NewCodexExecutor(&config.Config{})
			if streaming {
				result, err := executor.ExecuteStream(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else {
				if _, err := executor.Execute(ctx, auth, req, opts); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 2 {
				t.Fatalf("upstream calls=%d want 2", calls)
			}
			deadline := time.After(2 * time.Second)
			for {
				select {
				case record := <-plugin.records:
					if record.APIKey != key {
						continue
					}
					if record.Failed || record.ProviderAttempts != 2 {
						t.Fatalf("recovered request misreported: failed=%v attempts=%d", record.Failed, record.ProviderAttempts)
					}
					return
				case <-deadline:
					t.Fatal("recovered outcome was not published")
				}
			}
		})
	}
}

func TestXAICompletedStreamDoesNotFailOnConnectionCleanupError(t *testing.T) {
	ctx := context.WithValue(context.Background(), util.ContextKeyRoundTripper, xaiStreamRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		data := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
		reader := io.MultiReader(strings.NewReader(data), &terminalReadError{err: errors.New("stream error: INTERNAL_ERROR; received from peer")})
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(reader), Request: r}, nil
	}))
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": "https://test.invalid", "api_key": "test"}}
	result, err := NewXAIExecutor(&config.Config{}).ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "grok-4.7", Payload: []byte(`{"model":"grok-4.7","input":"hi","stream":true}`)}, cliproxyexecutor.Options{Stream: true, SourceFormat: sdktranslator.FromString("openai-response")})
	if err != nil {
		t.Fatal(err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("completed generation misclassified as failed: %v", chunk.Err)
		}
	}
}
