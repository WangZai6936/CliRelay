package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

// GetRequestOutcomes exposes explicit request lifecycle evidence independently
// of provider-attempt billing logs. Existing management auth scopes the tenant.
func (h *UsageLogsHandler) GetRequestOutcomes(c *gin.Context) {
	tenantID := identity.SystemTenantID
	if principal, ok := principalFromContext(c); ok && strings.TrimSpace(principal.EffectiveTenant.ID) != "" {
		tenantID = principal.EffectiveTenant.ID
	}
	params := usage.LogQueryParams{TenantID: tenantID, Page: intQueryDefault(c, "page", 1), Size: intQueryDefault(c, "size", 50), Days: intQueryDefault(c, "days", 7),
		APIKeys: queryStringListMulti(c, "api_key", "api_keys"), Models: queryStringListMulti(c, "model", "models")}
	results := queryStringListMulti(c, "result", "results")
	payload, err := usage.QueryRequestOutcomes(params, results)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "invalid result filter") {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, payload)
}
