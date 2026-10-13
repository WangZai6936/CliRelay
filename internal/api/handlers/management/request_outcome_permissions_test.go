package management

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
)

func TestRequestOutcomesUsesTenantRequestLogPermission(t *testing.T) {
	path := "/v0/management/request-outcomes"
	p := identity.Principal{EffectiveTenant: identity.Tenant{ID: "business-tenant"}}
	if deniesTenantResourceScope(p, path) {
		t.Fatal("request outcomes must be available in the active tenant")
	}
	if got := permissionForManagementRequest(http.MethodGet, path); got != "request_logs.read" {
		t.Fatalf("permission=%q", got)
	}
}
