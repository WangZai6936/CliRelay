package handlers

import (
	"context"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type recoverySequenceExecutor struct {
	mu           sync.Mutex
	modes        []string
	auths        []string
	beforeOutput time.Duration
}

func (e *recoverySequenceExecutor) Identifier() string { return "codex" }
func (e *recoverySequenceExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	panic("unused")
}
func (e *recoverySequenceExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	panic("unused")
}
func (e *recoverySequenceExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	panic("unused")
}
func (e *recoverySequenceExecutor) Refresh(_ context.Context, a *coreauth.Auth) (*coreauth.Auth, error) {
	return a, nil
}
func (e *recoverySequenceExecutor) ExecuteStream(ctx context.Context, a *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	i := len(e.auths)
	e.auths = append(e.auths, a.ID)
	mode := "unexpected-extra-attempt"
	if i < len(e.modes) {
		mode = e.modes[i]
	}
	e.mu.Unlock()
	err := &coreauth.Error{HTTPStatus: 502, Code: "server_is_overloaded", Message: "server_is_overloaded"}
	if mode == "http503" {
		err = &coreauth.Error{HTTPStatus: 503, Code: "temporarily_unavailable", Message: "maintenance"}
	}
	if mode == "http507" || mode == "sse507" || mode == "partial507" {
		err = &coreauth.Error{HTTPStatus: 507, Message: "exceeded request buffer limit while retrying upstream"}
	}
	if mode == "http" || mode == "http503" || mode == "http507" {
		return nil, err
	}
	ch := make(chan coreexecutor.StreamChunk, 2)
	go func() {
		defer close(ch)
		if mode == "ok" && e.beforeOutput > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.beforeOutput):
			}
		}
		if mode == "ok" || mode == "partial" || mode == "partial507" {
			ch <- coreexecutor.StreamChunk{Payload: []byte("ok")}
		}
		if mode != "ok" {
			ch <- coreexecutor.StreamChunk{Err: err}
		}
	}()
	return &coreexecutor.StreamResult{Chunks: ch}, nil
}

func newRecoverySequenceHandler(t *testing.T, e *recoverySequenceExecutor, disableCooling bool) (*BaseAPIHandler, *coreauth.Manager) {
	t.Helper()
	m := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	m.SetRetryConfig(3, 30*time.Second)
	m.RegisterExecutor(e)
	for _, id := range []string{"recovery-a", "recovery-b"} {
		a := &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"disable_cooling": disableCooling}}
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "recovery-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 20, MaxAttempts: 4, RecoveryTimeoutSeconds: 20}}, m)
	return h, m
}

func consumeRecoverySequence(t *testing.T, h *BaseAPIHandler, ctx context.Context) (string, *StreamErrorMessage) {
	t.Helper()
	data, _, errs := h.ExecuteStreamWithAuthManager(ctx, "openai", "recovery-model", []byte(`{"model":"recovery-model"}`), "")
	var output string
	if data != nil {
		for payload := range data {
			output += string(payload)
		}
	}
	var terminal *StreamErrorMessage
	for err := range errs {
		if err != nil {
			terminal = err
		}
	}
	return output, terminal
}

func TestStreamRecoverySharesActualAttemptBudgetAcrossHTTPAndSSE(t *testing.T) {
	for _, modes := range [][]string{{"http", "sse", "http", "sse", "ok"}, {"sse", "http", "sse", "http", "ok"}, {"http", "http", "sse", "sse", "ok"}} {
		t.Run(modes[0]+"-"+modes[1]+"-"+modes[2], func(t *testing.T) {
			e := &recoverySequenceExecutor{modes: modes}
			h, _ := newRecoverySequenceHandler(t, e, true)
			output, err := consumeRecoverySequence(t, h, context.Background())
			if output != "" || err == nil {
				t.Fatalf("output=%q err=%v, want final failure", output, err)
			}
			if len(e.auths) != 4 {
				t.Fatalf("actual calls=%d want4 (HTTP and SSE share cap)", len(e.auths))
			}
			if !reflect.DeepEqual(e.auths, []string{"recovery-a", "recovery-b", "recovery-a", "recovery-b"}) {
				t.Fatalf("auth order=%v, want immediate alternate account", e.auths)
			}
		})
	}
}

func TestStreamRecoveryWaitsForCoolingPoolThenSucceeds(t *testing.T) {
	for _, modes := range [][]string{{"http", "sse", "ok"}, {"http503", "http503", "ok"}} {
		t.Run(modes[0]+"-"+modes[1], func(t *testing.T) {
			e := &recoverySequenceExecutor{modes: modes}
			h, _ := newRecoverySequenceHandler(t, e, false)
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			output, err := consumeRecoverySequence(t, h, ctx)
			if output != "ok" || err != nil {
				t.Fatalf("output=%q err=%v", output, err)
			}
			if len(e.auths) != 3 {
				t.Fatalf("calls=%d want3", len(e.auths))
			}
			if elapsed := time.Since(started); elapsed < 5*time.Second || elapsed > 7*time.Second {
				t.Fatalf("cooling wait=%v want5..7s", elapsed)
			}
		})
	}
}

func TestStreamRecoveryDoesNotWaitPastClientDeadline(t *testing.T) {
	e := &recoverySequenceExecutor{modes: []string{"http", "sse", "ok"}}
	h, _ := newRecoverySequenceHandler(t, e, false)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := consumeRecoverySequence(t, h, ctx)
	if err == nil {
		t.Fatal("expected overload, not a wait exceeding client deadline")
	}
	if len(e.auths) != 2 || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("calls=%d elapsed=%v", len(e.auths), time.Since(started))
	}
}

func TestStreamRecoveryNeverReplaysAfterOutput(t *testing.T) {
	e := &recoverySequenceExecutor{modes: []string{"partial", "ok"}}
	h, _ := newRecoverySequenceHandler(t, e, true)
	output, err := consumeRecoverySequence(t, h, context.Background())
	if output != "ok" || err == nil || len(e.auths) != 1 {
		t.Fatalf("output=%q err=%v calls=%d", output, err, len(e.auths))
	}
}

func TestStreamRecoveryDoesNotTimeoutHealthyRetryGeneration(t *testing.T) {
	e := &recoverySequenceExecutor{modes: []string{"sse", "ok"}, beforeOutput: 1100 * time.Millisecond}
	h, _ := newRecoverySequenceHandler(t, e, true)
	h.Cfg.Streaming.RecoveryTimeoutSeconds = 1
	output, err := consumeRecoverySequence(t, h, context.Background())
	if output != "ok" || err != nil || len(e.auths) != 2 {
		t.Fatalf("output=%q err=%v calls=%d", output, err, len(e.auths))
	}
}

func TestBufferRecoveryAcrossHTTPAndSSEStopsAfterOneAlternative(t *testing.T) {
	for _, modes := range [][]string{{"http507", "ok"}, {"sse507", "ok"}, {"http507", "http507", "ok"}, {"sse507", "sse507", "ok"}, {"http507", "sse507", "ok"}, {"partial507", "ok"}} {
		t.Run(modes[0]+"-"+modes[1], func(t *testing.T) {
			e := &recoverySequenceExecutor{modes: modes}
			h, _ := newRecoverySequenceHandler(t, e, false)
			output, err := consumeRecoverySequence(t, h, context.Background())
			if modes[0] == "partial507" {
				if output != "ok" || err == nil || len(e.auths) != 1 {
					t.Fatalf("partial replay: output=%q err=%v calls=%v", output, err, e.auths)
				}
				return
			}
			if len(e.auths) != 2 || e.auths[0] == e.auths[1] {
				t.Fatalf("calls=%v", e.auths)
			}
			if modes[1] == "ok" {
				if output != "ok" || err != nil {
					t.Fatalf("output=%q err=%v", output, err)
				}
			} else if output != "" || err == nil {
				t.Fatalf("output=%q err=%v", output, err)
			}
		})
	}
}

func TestMeaningfulStreamOutputExcludesStartupAndTerminalFrames(t *testing.T) {
	for _, payload := range []string{": keep-alive\n\n", `data: {"choices":[{"delta":{"role":"assistant"}}]}`, `data: {"type":"response.created"}`, `data: {"usage":{"output_tokens":1}}`, "data: [DONE]", `data: {"type":"response.completed"}`} {
		if meaningfulStreamOutput([]byte(payload)) {
			t.Fatalf("startup/terminal counted as visible output: %s", payload)
		}
	}
	for _, payload := range []string{`data: {"choices":[{"delta":{"content":"hello"}}]}`, `data: {"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{"}}]}}]}`, `data: {"type":"response.output_text.delta","delta":"hello"}`, `data: {"type":"content_block_delta","delta":{"partial_json":"{"}}}`} {
		if !meaningfulStreamOutput([]byte(payload)) {
			t.Fatalf("visible output missing: %s", payload)
		}
	}
}
