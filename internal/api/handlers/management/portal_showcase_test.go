package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
)

func showcaseRequest(h *Handler, method, body string, system bool) *httptest.ResponseRecorder {
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if system {
			c.Set(managementPrincipalKey, identity.Principal{PlatformAdmin: true})
		}
		c.Next()
	}, CaptureConfigVersion())
	router.GET("/models", h.GetPublicPortalShowcase)
	router.GET("/settings", h.GetPortalShowcase)
	router.PUT("/settings", h.PutPortalShowcase)
	path := "/settings"
	if method == "public" {
		method = http.MethodGet
		path = "/models"
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, r)
	return w
}

func TestPortalShowcaseSavedAndPublishedWithoutPrivateConfig(t *testing.T) {
	setupOwnedPublicUsageDB(t)
	h := NewHandler(&config.Config{PortalRegistration: config.PortalRegistrationConfig{TenantID: "private-tenant"}}, filepath.Join(t.TempDir(), "config.yaml"), nil)
	t.Cleanup(h.Close)
	request := `{"items":[{"name":" GPT ","icon":"gpt"},{"name":"Claude","icon":"claude"}]}`
	if w := showcaseRequest(h, http.MethodPut, request, true); w.Code != http.StatusOK {
		t.Fatalf("save: %d %s", w.Code, w.Body)
	}
	// A second handler starts with an empty config, so this also verifies that
	// public display is restored from the shared settings store.
	other := NewHandler(&config.Config{}, "", nil)
	t.Cleanup(other.Close)
	w := showcaseRequest(other, "public", "", false)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("public: %d", w.Code)
	}
	var published struct {
		Items []config.PortalShowcaseModel `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &published); err != nil || len(published.Items) != 2 || published.Items[0].Name != "GPT" || published.Items[1].Name != "Claude" {
		t.Fatalf("published: %s / %v", w.Body, err)
	}
	for _, secret := range []string{"private-tenant", "password", "available_icons", "api-key", "version"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("public response leaks %s", secret)
		}
	}
	settings := showcaseRequest(h, http.MethodGet, "", true)
	var current struct {
		Version int64 `json:"version"`
	}
	_ = json.Unmarshal(settings.Body.Bytes(), &current)
	if current.Version < 1 {
		t.Fatalf("missing version: %s", settings.Body)
	}
	if w := showcaseRequest(h, http.MethodPut, `{"items":[],"version":0}`, true); w.Code != http.StatusConflict {
		t.Fatalf("stale write: %d %s", w.Code, w.Body)
	}
	if w := showcaseRequest(h, http.MethodPut, `{"items":[]}`, true); w.Code != http.StatusOK {
		t.Fatalf("clear: %d %s", w.Code, w.Body)
	}
	if w := showcaseRequest(other, "public", "", false); !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Fatalf("clear not published: %s", w.Body)
	}
}

func TestPortalShowcaseRejectsUntrustedInputAndTenantEdits(t *testing.T) {
	setupOwnedPublicUsageDB(t)
	h := NewHandler(&config.Config{}, filepath.Join(t.TempDir(), "config.yaml"), nil)
	t.Cleanup(h.Close)
	for _, body := range []string{`{}`, `{"items":null}`, `{"items":[{"name":"","icon":"gpt"}]}`, `{"items":[{"name":"GPT","icon":"https://example.com/logo.svg"}]}`, `{"items":[{"name":"GPT","icon":"gpt"},{"name":"gpt","icon":"gpt"}]}`, `{"items":[],"tenant_id":"other"}`, `{"items":[]} {}`} {
		if w := showcaseRequest(h, http.MethodPut, body, true); w.Code != http.StatusBadRequest {
			t.Fatalf("accepted %s: %d %s", body, w.Code, w.Body)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		if w := showcaseRequest(h, method, `{"items":[]}`, false); w.Code != http.StatusForbidden {
			t.Fatalf("non-system request: %d", w.Code)
		}
	}
}
