package openai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

type startupKeepAliveExecutor struct {
	start   func(context.Context, coreexecutor.Request) (*coreexecutor.StreamResult, error)
	cooling bool
}

type startupOutcomeCapture struct{ records chan usage.Record }

func (p *startupOutcomeCapture) HandleUsage(_ context.Context, r usage.Record) {
	if r.RequestOutcome != nil {
		select {
		case p.records <- r:
		default:
		}
	}
}

func (*startupKeepAliveExecutor) Identifier() string { return "startup-test" }
func (*startupKeepAliveExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unexpected nonstream execution")
}
func (e *startupKeepAliveExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return e.start(ctx, req)
}
func (*startupKeepAliveExecutor) Refresh(_ context.Context, a *coreauth.Auth) (*coreauth.Auth, error) {
	return a, nil
}
func (*startupKeepAliveExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unused")
}
func (*startupKeepAliveExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("unused")
}

func startupKeepAliveSuccess(path string) *coreexecutor.StreamResult {
	ch := make(chan coreexecutor.StreamChunk, 1)
	payload := `{"choices":[{"delta":{"content":"ready"}}]}`
	if path == "/v1/responses" {
		payload = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	}
	ch <- coreexecutor.StreamChunk{Payload: []byte(payload)}
	close(ch)
	return &coreexecutor.StreamResult{Chunks: ch}
}

func newStartupKeepAliveServer(t *testing.T, cfg *sdkconfig.SDKConfig, e *startupKeepAliveExecutor, setup func(*gin.Context)) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	m := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	m.SetRetryConfig(0, 0)
	if cfg.Streaming.MaxAttempts > 0 {
		m.SetRetryConfig(3, 30*time.Second)
	}
	m.RegisterExecutor(e)
	for _, id := range []string{"startup-a", "startup-b"} {
		a := &coreauth.Auth{ID: id, Provider: e.Identifier(), Status: coreauth.StatusActive, Metadata: map[string]any{"disable_cooling": !e.cooling}}
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, a.Provider, []*registry.ModelInfo{{ID: "startup-model"}, {ID: "startup-target"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	base := handlers.NewBaseAPIHandlers(cfg, m)
	chat := NewOpenAIAPIHandler(base)
	responses := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	if setup != nil {
		router.Use(func(c *gin.Context) { setup(c); c.Next() })
	}
	router.POST("/v1/chat/completions", chat.ChatCompletions)
	router.POST("/v1/completions", chat.Completions)
	router.POST("/v1/responses", responses.Responses)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

func startupKeepAlivePost(t *testing.T, ctx context.Context, server *httptest.Server, path string) (*http.Response, time.Time) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, strings.NewReader(`{"model":"startup-model","stream":true,"messages":[{"role":"user","content":"test"}],"prompt":"test","input":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	started := time.Now()
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, started
}

func TestOpenAIStartupKeepAliveWhileInitializing(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/completions"} {
		t.Run(path, func(t *testing.T) {
			release := make(chan struct{})
			e := &startupKeepAliveExecutor{start: func(ctx context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
				select {
				case <-release:
					return startupKeepAliveSuccess(path), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
			server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1}}, e, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, started := startupKeepAlivePost(t, ctx, server, path)
			line := make([]byte, len(": keep-alive\n\n"))
			_, err := io.ReadFull(resp.Body, line)
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			if string(line) != ": keep-alive\n\n" || time.Since(started) > 2500*time.Millisecond {
				t.Fatalf("first=%q after %v", line, time.Since(started))
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
				t.Fatalf("status=%d headers=%v", resp.StatusCode, resp.Header)
			}
			if path == "/v1/responses" && !strings.Contains(string(body), "response.completed") {
				t.Fatalf("body=%s", body)
			}
			if path != "/v1/responses" && !strings.Contains(string(body), "ready") {
				t.Fatalf("body=%s", body)
			}
		})
	}
}

func TestOpenAIStartupKeepAliveErrorUsesCommittedProtocol(t *testing.T) {
	capture := &startupOutcomeCapture{records: make(chan usage.Record, 32)}
	usage.RegisterPlugin(capture)
	for _, tc := range []struct {
		name, path string
		interval   int
		delay      time.Duration
		status     int
		sse        bool
	}{
		{"before-heartbeat", "/v1/chat/completions", 1, 0, 503, false},
		{"provider-499", "/v1/chat/completions", 1, 0, 499, false},
		{"disabled", "/v1/chat/completions", -1, 1100 * time.Millisecond, 503, false},
		{"chat-after-heartbeat", "/v1/chat/completions", 1, 1100 * time.Millisecond, 200, true},
		{"responses-after-heartbeat", "/v1/responses", 1, 1100 * time.Millisecond, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := usage.BeginRequest(context.Background())
			e := &startupKeepAliveExecutor{start: func(ctx context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
				select {
				case <-time.After(tc.delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				status := 503
				if tc.name == "provider-499" {
					status = 499
				}
				return nil, &coreauth.Error{Code: "invalid_request", Message: "startup rejected", HTTPStatus: status}
			}}
			server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: tc.interval}}, e, func(c *gin.Context) {
				c.Request = c.Request.WithContext(usage.WithRequestTracker(c.Request.Context(), tracker))
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, _ := startupKeepAlivePost(t, ctx, server, tc.path)
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status || !strings.Contains(string(body), "startup rejected") {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			if tc.sse {
				if !strings.Contains(string(body), ": keep-alive\n\n") || !strings.Contains(string(body), "data: ") {
					t.Fatalf("not SSE: %q", body)
				}
				if tc.path == "/v1/responses" && !strings.Contains(string(body), "event: error\n") {
					t.Fatalf("responses error=%q", body)
				}
			} else if strings.Contains(string(body), "keep-alive") || !strings.HasPrefix(string(body), `{"error":`) {
				t.Fatalf("expected plain JSON: %q", body)
			}
			usage.FinishRequest(tracker)
			for {
				select {
				case record := <-capture.records:
					if record.RequestOutcome.RequestID != usage.RequestIdentity(tracker) {
						continue
					}
					outcome := record.RequestOutcome
					if outcome.Result != usage.OutcomeFailed || outcome.FailureStage != usage.FailureBeforeOutput {
						t.Fatalf("heartbeat changed final outcome: %+v", outcome)
					}
					return
				case <-time.After(time.Second):
					t.Fatal("request outcome missing")
				}
			}
		})
	}
}

func TestOpenAIStartupKeepAliveCancellationWaitsForWorker(t *testing.T) {
	entered, exited, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	e := &startupKeepAliveExecutor{start: func(ctx context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
		close(entered)
		<-ctx.Done()
		defer close(exited)
		return nil, ctx.Err()
	}}
	server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1}}, e, func(c *gin.Context) { defer close(returned); c.Next() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"startup-model","stream":true}`))
		resp, err := server.Client().Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after cancellation")
	}
	select {
	case <-exited:
	default:
		t.Fatal("handler returned before worker stopped")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP client remained blocked")
	}
}

type startupModelError struct{}

func (startupModelError) Error() string   { return "model not found" }
func (startupModelError) StatusCode() int { return 404 }
func (startupModelError) UpstreamErrorBody() []byte {
	return []byte(`{"error":{"code":"model_not_found"}}`)
}

func TestOpenAIStartupKeepAlivePreservesFallbackAndDiagnostics(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprint(slow), func(t *testing.T) {
			var calls atomic.Int32
			requestContexts := make(chan *gin.Context, 1)
			e := &startupKeepAliveExecutor{start: func(ctx context.Context, req coreexecutor.Request) (*coreexecutor.StreamResult, error) {
				calls.Add(1)
				c := ctx.Value(requestctx.ContextKeyGin).(*gin.Context)
				c.Set("startup-diagnostic", "preserved")
				if req.Model == "startup-model" {
					if slow {
						select {
						case <-time.After(1100 * time.Millisecond):
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return nil, startupModelError{}
				}
				return startupKeepAliveSuccess("/v1/chat/completions"), nil
			}}
			cfg := &sdkconfig.SDKConfig{TextModelFallback: "startup-target", Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1, MaxAttempts: 4, RecoveryTimeoutSeconds: 20}}
			server := newStartupKeepAliveServer(t, cfg, e, func(c *gin.Context) {
				requestContexts <- c
				c.Set(handlers.RequestModelResolverContextKey, handlers.RequestModelResolver(func(model string, _ []byte, reason string) handlers.RequestModelResolution {
					return handlers.RequestModelResolution{RequestedModel: model, Model: "startup-target", Reason: reason}
				}))
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, _ := startupKeepAlivePost(t, ctx, server, "/v1/chat/completions")
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			requestGin := <-requestContexts
			if !strings.Contains(string(body), "ready") || calls.Load() != 3 {
				t.Fatalf("calls=%d body=%s", calls.Load(), body)
			}
			if !slow && resp.Header.Get("X-Clirelay-Resolved-Model") != "startup-target" {
				t.Fatalf("fallback headers=%v", resp.Header)
			}
			if value, _ := requestGin.Get("startup-diagnostic"); value != "preserved" {
				t.Fatalf("diagnostic=%v", value)
			}
			if value, _ := requestGin.Get(handlers.RequestModelResolutionContextKey); value == nil {
				t.Fatal("resolution lost")
			}
		})
	}
}

func TestOpenAIStartupKeepAliveDoesNotLockRecoveryBudget(t *testing.T) {
	var calls atomic.Int32
	e := &startupKeepAliveExecutor{start: func(ctx context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
		if calls.Add(1) == 1 {
			select {
			case <-time.After(1100 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, &coreauth.Error{HTTPStatus: 503, Code: "server_is_overloaded", Message: "server_is_overloaded"}
		}
		return startupKeepAliveSuccess("/v1/chat/completions"), nil
	}}
	server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1, MaxAttempts: 4, RecoveryTimeoutSeconds: 20}}, e, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, _ := startupKeepAlivePost(t, ctx, server, "/v1/chat/completions")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || !strings.Contains(string(body), ": keep-alive\n\n") || !strings.Contains(string(body), "ready") {
		t.Fatalf("calls=%d body=%s", calls.Load(), body)
	}
}

func TestOpenAIStartupKeepAliveDuringCooldownRecovery(t *testing.T) {
	var calls atomic.Int32
	e := &startupKeepAliveExecutor{cooling: true, start: func(_ context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
		if calls.Add(1) <= 2 {
			return nil, &coreauth.Error{HTTPStatus: 503, Code: "server_is_overloaded", Message: "server_is_overloaded"}
		}
		return startupKeepAliveSuccess("/v1/chat/completions"), nil
	}}
	server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1, MaxAttempts: 4, RecoveryTimeoutSeconds: 20}}, e, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	resp, started := startupKeepAlivePost(t, ctx, server, "/v1/chat/completions")
	heartbeat := make([]byte, len(": keep-alive\n\n"))
	if _, err := io.ReadFull(resp.Body, heartbeat); err != nil {
		t.Fatal(err)
	}
	if string(heartbeat) != ": keep-alive\n\n" || time.Since(started) > 2500*time.Millisecond {
		t.Fatalf("no heartbeat during account cooldown: %q at %v", heartbeat, time.Since(started))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || !strings.Contains(string(body), "ready") || time.Since(started) < 5*time.Second {
		t.Fatalf("calls=%d duration=%v body=%s", calls.Load(), time.Since(started), body)
	}
}

func TestOpenAIStartupKeepAliveDefaultAndDisabledIntervals(t *testing.T) {
	if got := handlers.StreamingKeepAliveInterval(&sdkconfig.SDKConfig{}); got != 15*time.Second {
		t.Fatalf("default=%v", got)
	}
	if got := handlers.StreamingKeepAliveInterval(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: -1}}); got != 0 {
		t.Fatalf("disabled=%v", got)
	}
}

type panickingStartupWriter struct {
	gin.ResponseWriter
	flush bool
}

func (w panickingStartupWriter) Write(p []byte) (int, error) {
	if !w.flush {
		panic("injected heartbeat write panic")
	}
	return w.ResponseWriter.Write(p)
}
func (w panickingStartupWriter) Flush() { panic("injected heartbeat flush panic") }

func TestOpenAIStartupKeepAliveOwnerPanicJoinsWorker(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(fmt.Sprint(flush), func(t *testing.T) {
			exited := make(chan struct{})
			observed := make(chan bool, 1)
			e := &startupKeepAliveExecutor{start: func(ctx context.Context, _ coreexecutor.Request) (*coreexecutor.StreamResult, error) {
				<-ctx.Done()
				close(exited)
				return nil, ctx.Err()
			}}
			server := newStartupKeepAliveServer(t, &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1}}, e, func(c *gin.Context) {
				c.Writer = panickingStartupWriter{ResponseWriter: c.Writer, flush: flush}
				defer func() {
					if recover() != nil {
						select {
						case <-exited:
							observed <- true
						default:
							observed <- false
						}
					}
				}()
				c.Next()
			})
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			resp, _ := startupKeepAlivePost(t, ctx, server, "/v1/chat/completions")
			_, _ = io.ReadAll(resp.Body)
			select {
			case joined := <-observed:
				if !joined {
					t.Fatal("HTTP panic recovered before startup worker exited")
				}
			case <-time.After(time.Second):
				t.Fatal("owner panic not observed")
			}
		})
	}
}
