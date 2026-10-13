package management

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func TestCodexHTTPTransportPolicyAPIValidationCASAndRollback(t *testing.T) {
	usage.CloseDB()
	if err := usage.InitDB(filepath.Join(t.TempDir(), "transport-policy.db"), config.RequestLogStorageConfig{}, time.UTC); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(usage.CloseDB)
	node := newVersionedNode(t)
	node.engine.GET("/codex-http-transport", node.h.GetCodexHTTPTransport)
	node.engine.PUT("/codex-http-transport", node.h.PutCodexHTTPTransport)
	read := node.do(t, http.MethodGet, "/codex-http-transport", "", "")
	if read.Code != 200 || read.Header().Get("ETag") != `"0"` {
		t.Fatalf("initial policy: %d %s", read.Code, read.Body.String())
	}
	for _, body := range []string{`{}`, `{"http1-percent":null}`, `{"http1-percent":-1}`, `{"http1-percent":101}`, `{"http1-percent":5.5}`} {
		if rec := node.do(t, http.MethodPut, "/codex-http-transport", body, `"0"`); rec.Code != 400 {
			t.Fatalf("accepted invalid %s: %d", body, rec.Code)
		}
	}
	if rec := node.do(t, http.MethodPut, "/codex-http-transport", `{"http1-percent":5}`, `"0"`); rec.Code != 200 || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("enable: %d %s", rec.Code, rec.Body.String())
	}
	if node.h.cfg.CodexHTTPTransport.HTTP1Percent != 5 {
		t.Fatal("policy not applied to live config")
	}
	if rec := node.do(t, http.MethodPut, "/codex-http-transport", `{"http1-percent":0}`, `"0"`); rec.Code != 409 {
		t.Fatalf("stale policy write: %d %s", rec.Code, rec.Body.String())
	}
	if rec := node.do(t, http.MethodPut, "/codex-http-transport", `{"http1-percent":0}`, `"1"`); rec.Code != 200 || rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("rollback: %d %s", rec.Code, rec.Body.String())
	}
	if node.h.cfg.CodexHTTPTransport.HTTP1Percent != 0 {
		t.Fatal("rollback was not applied")
	}
}

func TestCodexHTTPTransportPolicyIsSystemScoped(t *testing.T) {
	path := "/v0/management/codex-http-transport"
	if ManagementRequestPermission("GET", path) != "system.config.read" || ManagementRequestPermission("PUT", path) != "system.config.write" {
		t.Fatal("experiment policy is not protected by system config permissions")
	}
	principal := identity.Principal{EffectiveTenant: identity.Tenant{ID: "22222222-2222-2222-2222-222222222222"}}
	if !deniesTenantResourceScope(principal, path) {
		t.Fatal("business tenant can change global protocol policy")
	}
	principal.PlatformAdmin = true
	if deniesTenantResourceScope(principal, path) {
		t.Fatal("platform administrator lost policy access after tenant switch")
	}
}
