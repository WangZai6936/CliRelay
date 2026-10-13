package executor_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"golang.org/x/net/http2"
)

type codexBootstrapRoundTripper func(*http.Request) (*http.Response, error)

func (f codexBootstrapRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestCodexStartupFailureUsesBoundedHandlerRetry(t *testing.T) {
	for _, format := range []string{"openai", "openai-response"} {
		for _, scenario := range []struct {
			name        string
			partial     bool
			reasoning   bool
			headerOnly  bool
			reset       bool
			alwaysFails bool
			endEarly    bool
			wantCalls   int32
			wantError   bool
		}{
			{name: "overload then success", wantCalls: 2},
			{name: "overload after output never replayed", partial: true, wantCalls: 1, wantError: true},
			{name: "repeated overload exhausts budget", alwaysFails: true, wantCalls: 2, wantError: true},
			{name: "startup EOF exhausts budget", alwaysFails: true, endEarly: true, wantCalls: 2, wantError: true},
			{name: "ignored reasoning then overload", reasoning: true, wantCalls: 2},
			{name: "ignored reasoning then EOF", reasoning: true, endEarly: true, wantCalls: 2},
			{name: "ignored reasoning exhausts budget", reasoning: true, alwaysFails: true, wantCalls: 2, wantError: true},
			{name: "reasoning header then overload", reasoning: true, headerOnly: true, wantCalls: 2},
			{name: "reasoning header then EOF", reasoning: true, headerOnly: true, endEarly: true, wantCalls: 2},
			{name: "reasoning header exhausts budget", reasoning: true, headerOnly: true, alwaysFails: true, wantCalls: 2, wantError: true},
			{name: "reasoning header then HTTP2 reset", reasoning: true, headerOnly: true, reset: true, wantCalls: 2},
			{name: "HTTP2 reset after output never replayed", partial: true, reset: true, wantCalls: 1, wantError: true},
		} {
			t.Run(format+"/"+scenario.name, func(t *testing.T) {
				wantCalls, wantError := scenario.wantCalls, scenario.wantError
				committedReasoning := scenario.reasoning && !scenario.headerOnly && format == "openai-response"
				committed := scenario.partial || committedReasoning
				if committedReasoning {
					// Responses forwards reasoning items, unlike Chat Completions.
					// Preserve that output and never replay an established stream.
					wantCalls, wantError = 1, true
				}
				cfg := &config.Config{AuthDir: t.TempDir()}
				cfg.Streaming.BootstrapRetries = 1
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(executor.NewCodexExecutor(cfg))
				for i := 0; i < 2; i++ {
					id := fmt.Sprintf("bootstrap-%s-%d", t.Name(), i)
					auth := &coreauth.Auth{
						ID: id, Provider: "codex", Status: coreauth.StatusActive,
						Attributes: map[string]string{"base_url": "https://bootstrap.invalid/backend-api/codex"},
						Metadata:   map[string]any{"access_token": "mock-only-token", "account_id": "mock-only-account"},
					}
					if _, err := manager.Register(context.Background(), auth); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-6.1-sol"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				var calls atomic.Int32
				transport := codexBootstrapRoundTripper(func(request *http.Request) (*http.Response, error) {
					attempt := calls.Add(1)
					responseID := fmt.Sprintf("resp_attempt_%d", attempt)
					events := fmt.Sprintf("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"%s\",\"model\":\"gpt-6.1-sol\",\"created_at\":1}}\n\nevent: response.in_progress\ndata: {\"type\":\"response.in_progress\"}\n\ndata: {\"type\":\"keepalive\"}\n\n", responseID)
					if attempt == 1 || scenario.alwaysFails {
						if scenario.reasoning {
							events += "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"rs_mock\",\"type\":\"reasoning\",\"encrypted_content\":\"opaque\",\"summary\":[],\"content\":[]}}\n\n"
							if !scenario.headerOnly {
								events += "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"thinking\"}\n\n"
							}
						}
						if scenario.partial {
							events += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
						}
						if !scenario.endEarly && !scenario.reset {
							if scenario.partial {
								events += "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"type\":\"service_unavailable_error\",\"message\":\"mock overloaded\"}}}\n\n"
							} else {
								events += "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\",\"type\":\"service_unavailable_error\",\"message\":\"mock overloaded\"}}\n\n"
							}
						}
					} else {
						events += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
					}
					var responseBody io.Reader = strings.NewReader(events)
					if scenario.reset && (attempt == 1 || scenario.alwaysFails) {
						reset := http2.StreamError{StreamID: 7, Code: http2.ErrCodeInternal, Cause: errors.New("received from peer")}
						responseBody = io.MultiReader(responseBody, iotest.ErrReader(reset))
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(responseBody), Request: request}, nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ctx = context.WithValue(ctx, util.ContextKeyRoundTripper, transport)
				payload := []byte(`{"model":"gpt-6.1-sol","stream":true,"messages":[{"role":"user","content":"hello"}]}`)
				if format == "openai-response" {
					payload = []byte(`{"model":"gpt-6.1-sol","stream":true,"input":"hello"}`)
				}
				handler := handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)
				data, _, errors := handler.ExecuteStreamWithAuthManager(ctx, format, "gpt-6.1-sol", payload, "")
				var output strings.Builder
				for chunk := range data {
					output.Write(chunk)
				}
				errorCount := 0
				for err := range errors {
					if err != nil {
						errorCount++
					}
				}
				if ctx.Err() != nil {
					t.Fatalf("stream did not complete: %v", ctx.Err())
				}
				if got := calls.Load(); got != wantCalls {
					t.Fatalf("upstream attempts = %d, want %d; output=%s", got, wantCalls, output.String())
				}
				if (errorCount > 0) != wantError {
					t.Fatalf("errors = %d, want error %v; output=%s", errorCount, wantError, output.String())
				}
				if wantError && !committed && output.Len() != 0 {
					t.Fatalf("startup/error frames escaped before retry: %s", output.String())
				}
				if scenario.partial && !strings.Contains(output.String(), "partial") {
					t.Fatalf("partial output was lost: %s", output.String())
				}
				if scenario.partial && !scenario.reset && format == "openai-response" && !strings.Contains(output.String(), "response.failed") {
					t.Fatalf("native failure event after output was lost: %s", output.String())
				}
				if committedReasoning && !strings.Contains(output.String(), "rs_mock") {
					t.Fatalf("passthrough reasoning was lost: %s", output.String())
				}
				if !wantError && (!strings.Contains(output.String(), "resp_attempt_2") || strings.Contains(output.String(), "resp_attempt_1") || !strings.Contains(output.String(), "ok")) {
					t.Fatalf("retry mixed failed and successful streams: %s", output.String())
				}
			})
		}
	}
}
