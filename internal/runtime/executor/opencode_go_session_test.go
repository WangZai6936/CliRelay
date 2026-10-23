package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	ex "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	tr "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestOpenCodeGoForwardsConversationSessionPerRequest(t *testing.T) {
	for _, model := range []string{"deepseek-v4-flash", "claude-sonnet-4-6"} {
		for _, stream := range []bool{false, true} {
			t.Run(model+map[bool]string{false: "/normal", true: "/stream"}[stream], func(t *testing.T) {
				var got string
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					got = r.Header.Get("X-Opencode-Session")
					if got == "" {
						http.Error(w, "missing x-opencode-session", 400)
						return
					}
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: [DONE]\n\n")
					} else if r.URL.Path == "/messages" {
						_, _ = io.WriteString(w, `{"id":"msg","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
					} else {
						_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
					}
				}))
				defer srv.Close()
				old := opencodeGoBaseURL
				opencodeGoBaseURL = srv.URL
				defer func() { opencodeGoBaseURL = old }()
				credential := &auth.Auth{ID: "shared-credential", Attributes: map[string]string{"api_key": "test"}}
				payload := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"test"}]}`)
				for _, session := range []string{"conversation-one", "conversation-two"} {
					req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
					req.Header.Set("X-Opencode-Session", session)
					ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
					ginCtx.Request = req
					ctx := context.WithValue(context.Background(), util.ContextKeyGin, ginCtx)
					opts := ex.Options{SourceFormat: tr.FormatOpenAI, Stream: stream}
					executor := NewOpenCodeGoExecutor(&config.Config{})
					if stream {
						result, err := executor.ExecuteStream(ctx, credential, ex.Request{Model: model, Payload: payload}, opts)
						if err != nil {
							t.Fatal(err)
						}
						for c := range result.Chunks {
							if c.Err != nil {
								t.Fatal(c.Err)
							}
						}
					} else {
						if _, err := executor.Execute(ctx, credential, ex.Request{Model: model, Payload: payload}, opts); err != nil {
							t.Fatal(err)
						}
					}
					if got != session {
						t.Fatalf("got session %q want %q", got, session)
					}
					if _, ok := credential.Attributes["header:X-Opencode-Session"]; ok {
						t.Fatal("request session leaked into shared credential")
					}
				}
			})
		}
	}
}

func TestOpenCodeGoInjectsSessionWhenHeaderMissing(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Opencode-Session")
		if got == "" {
			http.Error(w, "missing x-opencode-session", 400)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer srv.Close()
	old := opencodeGoBaseURL
	opencodeGoBaseURL = srv.URL
	defer func() { opencodeGoBaseURL = old }()

	credential := &auth.Auth{ID: "shared-credential", Attributes: map[string]string{"api_key": "test"}}
	payload := []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"test"}]}`)
	opts := ex.Options{SourceFormat: tr.FormatOpenAI}
	executor := NewOpenCodeGoExecutor(&config.Config{})
	if _, err := executor.Execute(context.Background(), credential, ex.Request{Model: "deepseek-v4-flash", Payload: payload}, opts); err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Fatal("expected injected X-Opencode-Session on upstream request")
	}
	if _, ok := credential.Attributes["header:X-Opencode-Session"]; ok {
		t.Fatal("request session leaked into shared credential")
	}
}

func TestOpenCodeGoHarnessSessionBecomesOpencodeSession(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Opencode-Session")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer srv.Close()
	old := opencodeGoBaseURL
	opencodeGoBaseURL = srv.URL
	defer func() { opencodeGoBaseURL = old }()

	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Deepseek-Harness-Session-Id", "ses_harness_42")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = req
	ctx := context.WithValue(context.Background(), util.ContextKeyGin, ginCtx)

	credential := &auth.Auth{ID: "shared-credential", Attributes: map[string]string{"api_key": "test"}}
	payload := []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"test"}]}`)
	opts := ex.Options{SourceFormat: tr.FormatOpenAI}
	executor := NewOpenCodeGoExecutor(&config.Config{})
	if _, err := executor.Execute(ctx, credential, ex.Request{Model: "deepseek-v4-flash", Payload: payload}, opts); err != nil {
		t.Fatal(err)
	}
	if got != "ses_harness_42" {
		t.Fatalf("got session %q want ses_harness_42", got)
	}
}

func TestOpenCodeGoAuthWithSessionHeaderDoesNotMutateSharedAuth(t *testing.T) {
	credential := &auth.Auth{ID: "shared", Attributes: map[string]string{"api_key": "k"}}
	scoped := opencodeGoAuthWithSessionHeader(context.Background(), credential, ex.Options{})
	if scoped == credential {
		t.Fatal("expected cloned auth when injecting session")
	}
	if _, ok := credential.Attributes["header:X-Opencode-Session"]; ok {
		t.Fatal("shared credential must not receive session header attribute")
	}
	if scoped.Attributes["header:X-Opencode-Session"] == "" {
		t.Fatal("scoped auth must carry synthesized session")
	}
}

func TestOpenCodeGoExplicitSessionNotOverridden(t *testing.T) {
	headers := make(http.Header)
	headers.Set("Session-Id", "from-session-id")
	headers.Set("X-Opencode-Session", "explicit-opencode")
	got := opencodeGoAuthWithSessionHeader(context.Background(), &auth.Auth{ID: "a"}, ex.Options{Headers: headers})
	if got.Attributes["header:X-Opencode-Session"] != "explicit-opencode" {
		t.Fatalf("got %q want explicit-opencode", got.Attributes["header:X-Opencode-Session"])
	}
}
