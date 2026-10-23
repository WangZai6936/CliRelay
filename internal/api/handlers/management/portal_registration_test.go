package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/enduser"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func signupRequest(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/portal/auth/register", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.RemoteAddr = "192.0.2.10:12345"
	h.PostPortalRegister(c)
	return recorder
}

func registrationHandler(t *testing.T) *Handler {
	t.Helper()
	setupOwnedPublicUsageDB(t)
	db := usage.RuntimeDB()
	if _, err := db.Exec(`INSERT INTO tenants (id, status, type) VALUES (?, 'active', 'system')`, identity.SystemTenantID); err != nil {
		t.Fatal(err)
	}
	enduser.SetDefault(enduser.NewService(db))
	h := &Handler{cfg: &config.Config{PortalRegistration: config.PortalRegistrationConfig{Enabled: true}}, loginThrottle: newLoginThrottle(nil)}
	t.Cleanup(h.Close)
	return h
}

func TestPublicRegistrationRequiresOptInAndDoesNotDisclosePolicy(t *testing.T) {
	h := &Handler{cfg: &config.Config{PortalRegistration: config.PortalRegistrationConfig{TenantID: "private-tenant", PermissionProfileID: "private-profile"}}}
	response := signupRequest(t, h, `{}`)
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "registration_disabled") {
		t.Fatalf("disabled registration: %d %s", response.Code, response.Body)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h.GetPortalRegistration(c)
	if strings.Contains(recorder.Body.String(), "private-") || !strings.Contains(recorder.Body.String(), `"enabled":false`) || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("public capabilities leak or caching: %s", recorder.Body)
	}
}

func TestPublicRegistrationCreatesUsableAccountWithoutLeakingCredentials(t *testing.T) {
	h := registrationHandler(t)
	response := signupRequest(t, h, `{"username":" Alice ","display_name":"Alice","password":"Signup-Password-123!"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("signup: %d %s", response.Code, response.Body)
	}
	var result struct {
		User enduser.User `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.User.Username != "alice" || result.User.TenantID != identity.SystemTenantID || result.User.MustChangePassword {
		t.Fatalf("public account: %+v", result.User)
	}
	for _, secret := range []string{"Signup-Password", "password_hash", "default_api_key", "sk-", "generated_password"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("signup response contains credential material %q", secret)
		}
	}
	response = signupRequest(t, h, `{"username":"ALICE","password":"Signup-Password-123!"}`)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "username_taken") {
		t.Fatalf("duplicate: %d %s", response.Code, response.Body)
	}
}

func TestPublicRegistrationRejectsClientPolicyAndMalformedJSON(t *testing.T) {
	h := registrationHandler(t)
	for _, body := range []string{
		`{"username":"alice","password":"Signup-Password-123!","tenant_id":"other"}`,
		`{"username":"alice","password":"Signup-Password-123!","permission_profile_id":"admin"}`,
		`{"username":"alice","password":"Signup-Password-123!"} {}`,
		`{"username":`,
	} {
		response := signupRequest(t, h, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("untrusted body: %d %s", response.Code, response.Body)
		}
	}
	var count int
	if err := usage.RuntimeDB().QueryRow(`SELECT COUNT(*) FROM end_users`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("untrusted body wrote accounts: %d / %v", count, err)
	}
}

func TestPublicRegistrationBodyLimitIgnoresSpoofedContentType(t *testing.T) {
	h := registrationHandler(t)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/portal/auth/register", strings.NewReader(`{"username":"alice","password":"`+strings.Repeat("a", 5000)+`"}`))
	c.Request.Header.Set("Content-Type", "multipart/form-data")
	c.Request.ContentLength = -1
	h.PostPortalRegister(c)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unbounded multipart signup: %d %s", recorder.Code, recorder.Body)
	}
}

func TestPublicRegistrationThrottlesBeforeDBAndHashing(t *testing.T) {
	h := &Handler{cfg: &config.Config{PortalRegistration: config.PortalRegistrationConfig{Enabled: true}}, loginThrottle: newLoginThrottle(nil)}
	t.Cleanup(h.Close)
	// Invalid JSON needs no runtime DB: after the attempt budget is spent the
	// rejection is 429, independently of database availability and password work.
	for attempt := 0; attempt < 5; attempt++ {
		if response := signupRequest(t, h, `{`); response.Code == http.StatusTooManyRequests {
			t.Fatalf("throttled attempt %d too early", attempt+1)
		}
	}
	response := signupRequest(t, h, `{`)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "" {
		t.Fatalf("signup throttle: %d %s", response.Code, response.Body)
	}
	if decision := h.loginThrottle.evaluate(throttleKey{Scope: scopePortalPassword, Value: "192.0.2.10/32"}, time.Now()); decision.Outcome != outcomeAllow {
		t.Fatal("registration consumed the portal password budget")
	}
}

func TestPublicRegistrationPageHasPrivateHeadersAndNoInjectedConfig(t *testing.T) {
	h := &Handler{}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h.GetPortalRegistrationPage(c)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "id=\"registration\"") || recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("X-Frame-Options") != "DENY" || !strings.Contains(recorder.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("registration page response: %d %v", recorder.Code, recorder.Header())
	}
}
