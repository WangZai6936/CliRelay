package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestGetRequestOutcomesFiltersTenantAndResultWithoutExposingSecrets(t *testing.T) {
	initMonitorHandlerDB(t)
	now := time.Now().UTC()
	for _, tenant := range []string{identity.SystemTenantID, "22222222-2222-2222-2222-222222222222"} {
		for _, status := range []string{coreusage.OutcomeRecovered, coreusage.OutcomeFailed} {
			usage.InsertRequestLog(usage.RequestLogEntry{TrustedTenantID: tenant, APIKey: "private-test-key", APIKeyID: "key-id", Model: "test-model", Timestamp: now,
				RequestOutcome: &coreusage.RequestOutcome{RequestID: tenant + status, StartedAt: now, CompletedAt: now, Result: status, Attempts: 3, FailedAttempts: 2}})
		}
	}
	for _, tc := range []struct {
		query  string
		status int
	}{{"?days=1&result=recovered", http.StatusOK}, {"?result=not-valid", http.StatusBadRequest}} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/request-outcomes"+tc.query, nil)
		c.Set(managementPrincipalKey, identity.Principal{EffectiveTenant: identity.Tenant{ID: "22222222-2222-2222-2222-222222222222"}})
		(&Handler{}).UsageLogs().GetRequestOutcomes(c)
		if rec.Code != tc.status {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "private-test-key") {
			t.Fatal("credential secret leaked")
		}
		if tc.status == http.StatusOK {
			var page usage.RequestOutcomeQueryResult
			if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if page.Total != 1 || page.Stats.RecoveredRequests != 1 || page.Stats.FinalFailedRequests != 0 || len(page.Items) != 1 {
				t.Fatalf("tenant/result leak: %+v", page)
			}
		}
	}
}
