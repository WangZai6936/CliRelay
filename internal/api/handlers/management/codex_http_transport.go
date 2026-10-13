package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/management/settings/runtimeconfig"
)

// GetCodexHTTPTransport reads the process-wide experiment policy and CAS
// version. Business-tenant selection never changes this setting's scope.
func (h *Handler) GetCodexHTTPTransport(c *gin.Context) {
	cfg := h.freshConfig(identity.SystemTenantID)
	setVersionHeader(c, cfg.RuntimeSettingState().Version(runtimeconfig.RuntimeSettingCodexHTTPTransport))
	c.JSON(http.StatusOK, gin.H{"http1-percent": cfg.CodexHTTPTransport.CanaryPercent()})
}

func (h *Handler) PutCodexHTTPTransport(c *gin.Context) {
	var body struct {
		HTTP1Percent *int `json:"http1-percent"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.HTTP1Percent == nil || *body.HTTP1Percent < 0 || *body.HTTP1Percent > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "http1-percent must be an integer between 0 and 100"})
		return
	}
	h.mutateSystemConfig(c, func(cfg *config.Config) error {
		cfg.CodexHTTPTransport = config.CodexHTTPTransportConfig{HTTP1Percent: *body.HTTP1Percent}
		return nil
	})
}
