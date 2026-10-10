package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func aliasTestServer() *Server {
	return &Server{cfg: &config.Config{SDKConfig: config.SDKConfig{RequestModelAliases: map[string]string{"jiezi": "gpt-6-luna", "gpt-6-sol": "gpt-6-luna"}}}}
}

func TestLegacyModelAliasResolvesBeforeAdmissionAndPreservesHistory(t *testing.T) {
	initDisabledModelTestDB(t)
	seedModelConfig(t, "gpt-6-sol", false)
	seedModelConfig(t, "gpt-6-luna", true)
	gin.SetMode(gin.TestMode)
	for _, alias := range []string{"jiezi", "gpt-6-sol"} {
		engine := gin.New()
		engine.Use(func(c *gin.Context) { c.Set("accessMetadata", map[string]string{"allowed-models": "gpt-6-luna"}) })
		engine.POST("/v1/responses", aliasTestServer().modelRestrictionMiddleware(), func(c *gin.Context) {
			raw, _ := io.ReadAll(c.Request.Body)
			c.Data(http.StatusOK, "application/json", raw)
		})
		body := `{"model":"` + alias + `","input":[{"type":"message","role":"user","content":"continue"},{"type":"function_call","call_id":"c1","arguments":"{}"},{"type":"function_call_output","call_id":"c1","output":"result"}],"stream":true}`
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || gjson.Get(recorder.Body.String(), "model").String() != "gpt-6-luna" {
			t.Fatalf("alias=%s status=%d body=%s", alias, recorder.Code, recorder.Body)
		}
		if gjson.Get(recorder.Body.String(), "input").Raw != gjson.Get(body, "input").Raw || !gjson.Get(recorder.Body.String(), "stream").Bool() {
			t.Fatal("alias rewrite changed conversation/tools/stream")
		}
	}
}

func TestLegacyModelAliasCannotBypassTargetPermissionOrDisableToggle(t *testing.T) {
	initDisabledModelTestDB(t)
	seedModelConfig(t, "gpt-6-luna", true)
	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Set("accessMetadata", map[string]string{"allowed-models": "jiezi,gpt-6-sol"})
	gate := aliasTestServer().buildModelGate(context)
	if rejection := gate("jiezi"); rejection == nil || rejection.StatusCode != http.StatusForbidden {
		t.Fatal("legacy-name permission bypassed target permission")
	}
	context.Set("accessMetadata", map[string]string{"allowed-models": "gpt-6-luna"})
	seedModelConfig(t, "gpt-6-luna", false)
	gate = aliasTestServer().buildModelGate(context)
	if rejection := gate("gpt-6-sol"); rejection == nil || rejection.StatusCode != http.StatusNotFound {
		t.Fatal("alias bypassed target disable toggle")
	}
}
