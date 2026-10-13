package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	modelconfigsettings "github.com/router-for-me/CLIProxyAPI/v6/internal/management/settings/modelconfig"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkmodelcatalog "github.com/router-for-me/CLIProxyAPI/v6/sdk/modelcatalog"
	"github.com/tidwall/gjson"
)

type fallbackFixtureExecutor struct {
	quotaWSExecutor
	muCalls      sync.Mutex
	requests     []coreexecutor.Request
	fail         map[string]error
	streamErrors map[string]error
	partial      bool
}

func (e *fallbackFixtureExecutor) capture(req coreexecutor.Request) error {
	e.muCalls.Lock()
	defer e.muCalls.Unlock()
	e.requests = append(e.requests, req)
	return e.fail[req.Model]
}

func (e *fallbackFixtureExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	if err := e.capture(req); err != nil {
		return coreexecutor.Response{}, err
	}
	return coreexecutor.Response{Payload: []byte(fmt.Sprintf(`{"id":"ok","object":"response","model":%q,"output":[]}`, req.Model))}, nil
}

func (e *fallbackFixtureExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	if err := e.capture(req); err != nil {
		return nil, err
	}
	if err := e.streamErrors[req.Model]; err != nil {
		chunks := make(chan coreexecutor.StreamChunk, 2)
		if e.partial {
			chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"started\"}}\n\n")}
		}
		chunks <- coreexecutor.StreamChunk{Err: err}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"ok\",\"model\":%q,\"output\":[]}}\n\n", req.Model))}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestTextFallbackFirstStreamModelErrorRecoversButPartialOutputDoesNotReplay(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			server, exec := textFallbackFixture(t, "")
			exec.partial = partial
			exec.streamErrors = map[string]error{"working-text-model": &coreauth.Error{HTTPStatus: 404, Message: `{"error":{"code":"model_not_found","message":"model removed upstream"}}`}}
			engine := gin.New()
			engine.Use(func(c *gin.Context) {
				c.Set("accessMetadata", map[string]string{"allowed-models": "working-text-model,gpt-6-luna"})
				c.Next()
			})
			responses := openai.NewOpenAIResponsesAPIHandler(server.handlers)
			engine.POST("/v1/responses", server.modelRestrictionMiddleware(), responses.Responses)
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"working-text-model","input":"hi","stream":true}`))
			req.Header.Set("Content-Type", "application/json")
			r := httptest.NewRecorder()
			engine.ServeHTTP(r, req)
			exec.muCalls.Lock()
			defer exec.muCalls.Unlock()
			want := 2
			if partial {
				want = 1
			}
			if len(exec.requests) != want {
				t.Fatalf("partial=%v calls=%d body=%s", partial, len(exec.requests), r.Body)
			}
			if !partial && (!strings.Contains(r.Body.String(), "response.completed") || exec.requests[1].Model != "gpt-6-luna") {
				t.Fatalf("first model error was not recovered: %s", r.Body)
			}
		})
	}
}

func TestTextFallbackDoesNotEscapeAllowedChannelsOrTenant(t *testing.T) {
	server, _ := textFallbackFixture(t, "")
	for _, metadata := range []map[string]string{{"allowed-models": "gpt-6-luna", "allowed-channels": "different-provider"}, {"allowed-models": "gpt-6-luna", "tenant-id": "another-tenant"}} {
		engine := gin.New()
		engine.Use(func(c *gin.Context) {
			c.Set("accessMetadata", metadata)
			if tenant := metadata["tenant-id"]; tenant != "" {
				c.Set("tenantID", tenant)
			}
			c.Next()
		})
		engine.POST("/v1/responses", server.modelRestrictionMiddleware(), func(c *gin.Context) { c.Status(200) })
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"unregistered-model","input":"hi"}`))
		req.Header.Set("Content-Type", "application/json")
		r := httptest.NewRecorder()
		engine.ServeHTTP(r, req)
		if r.Header().Get("X-Clirelay-Resolved-Model") != "" {
			t.Fatal("fallback escaped request scope")
		}
	}
}

func textFallbackFixture(t *testing.T, allowed string) (*Server, *fallbackFixtureExecutor) {
	t.Helper()
	initDisabledModelTestDB(t)
	// This fixture replaces the whole database. Production invalidates the
	// disable cache on catalog writes; a DB replacement needs its own reset.
	modelconfigsettings.InvalidateDisabledModelCache()
	t.Cleanup(modelconfigsettings.InvalidateDisabledModelCache)
	gin.SetMode(gin.TestMode)
	provider := "fallback-" + strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	exec := &fallbackFixtureExecutor{quotaWSExecutor: quotaWSExecutor{provider: provider}, fail: map[string]error{}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetModelRegistry(sdkmodelcatalog.GlobalRegistry())
	manager.RegisterExecutor(exec)
	auth := &coreauth.Auth{ID: provider, Provider: provider, Status: coreauth.StatusActive, TenantID: identity.SystemTenantID}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, provider, []*registry.ModelInfo{{ID: "gpt-6-luna"}, {ID: "working-text-model"}, {ID: "denied-text-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	cfg := &config.Config{SDKConfig: config.SDKConfig{TextModelFallback: "gpt-6-luna"}}
	base := handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager)
	return &Server{cfg: cfg, handlers: base}, exec
}

func fallbackRequest(t *testing.T, server *Server, allowed, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	engine := gin.New()
	engine.Use(func(c *gin.Context) { c.Set("accessMetadata", map[string]string{"allowed-models": allowed}); c.Next() })
	engine.POST(path, server.modelRestrictionMiddleware(), func(c *gin.Context) { data, _ := io.ReadAll(c.Request.Body); c.Data(200, "application/json", data) })
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r := httptest.NewRecorder()
	engine.ServeHTTP(r, req)
	return r
}

func TestTextFallbackCoversNewNamesDisabledModelsAndMissingRoutes(t *testing.T) {
	server, _ := textFallbackFixture(t, "")
	seedModelConfig(t, "retired-text-model", false)
	seedModelConfig(t, "known-without-route", true)
	for _, model := range []string{"jiezi", "future-model-2027", "other-model-v999", "retired-text-model", "known-without-route"} {
		t.Run(model, func(t *testing.T) {
			body := fmt.Sprintf(`{"model":%q,"input":[{"role":"user","content":"continue"},{"type":"function_call","call_id":"c1","arguments":"{}"},{"type":"function_call_output","call_id":"c1","output":"done"}],"stream":true}`, model)
			r := fallbackRequest(t, server, "gpt-6-luna,known-without-route", "/v1/responses", body)
			if r.Code != 200 || gjson.Get(r.Body.String(), "model").String() != "gpt-6-luna" {
				t.Fatalf("status=%d response=%s", r.Code, r.Body)
			}
			if gjson.Get(r.Body.String(), "input").Raw != gjson.Get(body, "input").Raw || r.Header().Get("X-Clirelay-Model-Fallback") == "" {
				t.Fatal("history/diagnostic changed or missing")
			}
		})
	}
}

func TestTextFallbackKeepsHealthyAndUnauthorizedModelsAndUnavailableTarget(t *testing.T) {
	// Reproduce the shared-registry condition explicitly instead of depending
	// on Go map iteration or the order in which unrelated tests execute.
	const unrelatedClient = "text-fallback-unrelated-auto-client"
	registry.GetGlobalRegistry().RegisterClient(unrelatedClient, "unrelated-auto-provider", []*registry.ModelInfo{{ID: "unrelated-auto-model", Created: time.Now().Unix()}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(unrelatedClient) })
	server, _ := textFallbackFixture(t, "")
	checkHealthy := func(model string) {
		if model == "auto" {
			// Auto can select any process-wide registry candidate, including a
			// different one on a later resolution. Make each possible selection
			// routable for this healthy-auto case, then restore the narrow fixture
			// before checking denied models and an unavailable fallback target.
			reg := registry.GetGlobalRegistry()
			auth := server.handlers.AuthManager.List()[0]
			original := reg.GetModelsForClient(auth.ID)
			models := append([]*registry.ModelInfo(nil), original...)
			seen := make(map[string]bool, len(models))
			for _, info := range models {
				seen[info.ID] = true
			}
			for _, available := range reg.GetAvailableModels("") {
				id, _ := available["id"].(string)
				if !seen[id] {
					models = append(models, reg.GetModelInfo(id, ""))
					seen[id] = true
				}
			}
			reg.RegisterClient(auth.ID, auth.Provider, models)
			defer reg.RegisterClient(auth.ID, auth.Provider, original)
		}
		r := fallbackRequest(t, server, "", "/v1/responses", fmt.Sprintf(`{"model":%q,"input":"hi"}`, model))
		if r.Code != 200 || r.Header().Get("X-Clirelay-Resolved-Model") != "" {
			t.Fatalf("valid suffix/auto request was substituted: %s", model)
		}
	}
	for _, model := range []string{"working-text-model(high)", "auto"} {
		checkHealthy(model)
	}
	for _, tc := range []struct {
		model, allowed string
		status         int
		want           string
	}{
		{"working-text-model", "working-text-model,gpt-6-luna", 200, "working-text-model"},
		{"denied-text-model", "gpt-6-luna", 403, ""},
		{"unknown-model", "working-text-model", 403, ""},
	} {
		r := fallbackRequest(t, server, tc.allowed, "/v1/responses", fmt.Sprintf(`{"model":%q,"input":"hi"}`, tc.model))
		if r.Code != tc.status {
			t.Fatalf("model=%s status=%d body=%s", tc.model, r.Code, r.Body)
		}
		if tc.want != "" && gjson.Get(r.Body.String(), "model").String() != tc.want {
			t.Fatal("healthy model replaced")
		}
	}
	seedModelConfig(t, "gpt-6-luna", false)
	r := fallbackRequest(t, server, "gpt-6-luna", "/v1/responses", `{"model":"unknown-model","input":"hi"}`)
	if r.Code == 200 || r.Header().Get("X-Clirelay-Resolved-Model") != "" {
		t.Fatal("disabled fallback target was used")
	}
	seedModelConfig(t, "gpt-6-luna", true)
	server.handlers.AuthManager.SetModelRegistry(sdkmodelcatalog.GlobalRegistry())
	registry.GetGlobalRegistry().UnregisterClient(server.handlers.AuthManager.List()[0].ID)
	r = fallbackRequest(t, server, "gpt-6-luna", "/v1/responses", `{"model":"unknown-model","input":"hi"}`)
	if r.Code == 200 || r.Header().Get("X-Clirelay-Resolved-Model") != "" {
		t.Fatal("unroutable fallback target was used")
	}
}

func TestTextFallbackDoesNotConvertMediaEndpointsOrMultimodalInput(t *testing.T) {
	server, _ := textFallbackFixture(t, "")
	for _, tc := range []struct{ path, body string }{
		{"/v1/images/generations", `{"model":"unknown-model","prompt":"draw"}`},
		{"/v1/videos/generations", `{"model":"unknown-model","prompt":"animate"}`},
		{"/v1/responses/compact", `{"model":"unknown-model","input":"hi"}`},
		{"/v1/responses", `{"model":"unknown-model","input":"draw","tools":[{"type":"image_generation"}]}`},
		{"/v1/responses", `{"model":"unknown-model","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://test.invalid/image"}]}]}`},
		{"/v1/chat/completions", `{"model":"unknown-model","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"test","format":"wav"}}]}]}`},
	} {
		r := fallbackRequest(t, server, "gpt-6-luna", tc.path, tc.body)
		if r.Header().Get("X-Clirelay-Resolved-Model") != "" {
			t.Fatalf("non-text request was substituted: %s", tc.body)
		}
	}
}

func TestTextFallbackWebsocketResolvesEveryTurnAndClearsOldProviderPin(t *testing.T) {
	server, exec := textFallbackFixture(t, "")
	registry.GetGlobalRegistry().RegisterClient(exec.provider, exec.provider, []*registry.ModelInfo{{ID: "working-text-model"}, {ID: "denied-text-model"}})
	target := &fallbackFixtureExecutor{quotaWSExecutor: quotaWSExecutor{provider: exec.provider + "-target"}, fail: map[string]error{}}
	server.handlers.AuthManager.RegisterExecutor(target)
	if _, err := server.handlers.AuthManager.Register(context.Background(), &coreauth.Auth{ID: target.provider, Provider: target.provider, Status: coreauth.StatusActive, TenantID: identity.SystemTenantID}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(target.provider, target.provider, []*registry.ModelInfo{{ID: "gpt-6-luna"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(target.provider) })
	responses := openai.NewOpenAIResponsesAPIHandler(server.handlers)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set("accessMetadata", map[string]string{"allowed-models": "working-text-model,gpt-6-luna"})
		c.Next()
	})
	engine.GET("/v1/responses", server.modelRestrictionMiddleware(), responses.ResponsesWebsocket)
	srv := httptest.NewServer(engine)
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for _, model := range []string{"working-text-model", "new-missing-model", "another-missing-model", "working-text-model"} {
		payload, _ := json.Marshal(map[string]any{"type": "response.create", "model": model, "input": []any{map[string]any{"role": "user", "content": "hi"}}})
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Fatal(err)
		}
		_, response, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		event := gjson.GetBytes(response, "type").String()
		if event != "response.completed" && event != "response.done" {
			t.Fatalf("turn model=%s failed: %s", model, response)
		}
	}
	exec.muCalls.Lock()
	defer exec.muCalls.Unlock()
	target.muCalls.Lock()
	defer target.muCalls.Unlock()
	if len(exec.requests) != 2 || len(target.requests) != 2 || exec.requests[1].Model != "working-text-model" || target.requests[0].Model != "gpt-6-luna" || target.requests[1].Model != "gpt-6-luna" {
		t.Fatalf("wrong turn routing: source=%+v target=%+v", exec.requests, target.requests)
	}
}
