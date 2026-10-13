package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestOpenAICompatStreamOutcomes(t *testing.T) {
	const partial = `data: {"id":"test","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`
	const finish = `data: {"id":"test","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	const streamError = `data: {"error":{"message":"upstream failed","type":"server_error","code":"server_error"}}`
	cases := []struct {
		name, stream string
		failed       bool
	}{
		{"error_before_done", streamError + "\n\ndata: [DONE]\n\n", true},
		{"partial_eof", partial + "\n\n", true},
		{"empty_eof", "", true},
		{"usage_without_completion", partial + "\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n", true},
		{"error_after_finish", partial + "\n\n" + finish + "\n\n" + streamError + "\n\n", true},
		{"done", partial + "\n\ndata: [DONE]\n\n", false},
		{"finish_without_done", partial + "\n\n" + finish + "\n\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &usageCapturePlugin{records: make(chan cliproxyusage.Record, 8)}
			cliproxyusage.RegisterPlugin(plugin)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, tc.stream)
			}))
			defer server.Close()
			model := "stability-" + tc.name
			executor := NewOpenAICompatExecutor("stability-test", &config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL + "/v1", "api_key": "test-only"}}
			result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: model, Payload: []byte(`{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"test"}]}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI, Stream: true})
			if err != nil {
				t.Fatal(err)
			}
			var streamErr error
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					streamErr = chunk.Err
				}
			}
			if (streamErr != nil) != tc.failed {
				t.Fatalf("stream error = %v, expected failed=%v", streamErr, tc.failed)
			}
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			for {
				select {
				case record := <-plugin.records:
					if record.Model != model {
						continue
					}
					if record.Failed != tc.failed {
						t.Fatalf("usage failed=%v, want %v", record.Failed, tc.failed)
					}
					return
				case <-timer.C:
					t.Fatal("missing usage outcome")
				}
			}
		})
	}
}

func TestOpenAICompatNonStreamRejectsHTTP200Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"error":{"message":"upstream failed","type":"server_error"}}`)
	}))
	defer server.Close()
	executor := NewOpenAICompatExecutor("stability-test", &config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL + "/v1", "api_key": "test-only"}}
	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{Model: "stability-nonstream", Payload: []byte(`{"model":"stability-nonstream","messages":[{"role":"user","content":"test"}]}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI})
	if err == nil || !strings.Contains(err.Error(), "upstream failed") {
		t.Fatalf("HTTP 200 error was accepted: %v", err)
	}
}

func TestOpenAICompatStreamOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		payload string
		status  int
	}{
		{`{"error":{"code":"rate_limit_exceeded"}}`, 429},
		{`{"type":"response.failed","response":{"error":{"type":"invalid_request_error"}}}`, 400},
		{`{"success":false,"error":{"type":"server_error"}}`, 502},
	} {
		err := openAICompatResponseError([]byte(tc.payload))
		if err == nil || err.(interface{ StatusCode() int }).StatusCode() != tc.status {
			t.Fatalf("classification of %s = %v", tc.payload, err)
		}
	}
	if !openAICompatStreamCompleted([]byte(`{"choices":[{"index":1,"finish_reason":"tool_calls"}]}`)) {
		t.Fatal("nonzero choice finish was ignored")
	}
	if openAICompatResponseError([]byte(`{"error":null,"choices":[{"finish_reason":"length"}]}`)) != nil {
		t.Fatal("valid length finish rejected")
	}
}
