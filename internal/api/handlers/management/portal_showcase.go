package management

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/bodyutil"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/management/settings/runtimeconfig"
	settingsstore "github.com/router-for-me/CLIProxyAPI/v6/internal/management/settings/store"
)

var portalShowcaseIcons = []string{"claude", "gemini", "gpt", "grok", "qwen", "deepseek", "codex", "kimi", "glm", "minimax", "hunyuan", "mimo", "ollama", "vertex", "custom"}

func (h *Handler) publicShowcaseItems() []config.PortalShowcaseModel {
	h.mu.Lock()
	var items []config.PortalShowcaseModel
	if h.cfg != nil {
		items = append(items, h.cfg.PortalModelShowcase...)
	}
	h.mu.Unlock()
	// Public reads fetch only this small setting, never the complete private
	// runtime configuration. The live copy remains a fallback during DB outages.
	if payload, ok := settingsstore.GetRuntimeSettingPayload(runtimeconfig.RuntimeSettingPortalShowcase); ok {
		var stored []config.PortalShowcaseModel
		if json.Unmarshal(payload, &stored) == nil {
			items = stored
		}
	}
	if len(items) == 0 {
		return []config.PortalShowcaseModel{}
	}
	return append([]config.PortalShowcaseModel(nil), items...)
}

// GetPublicPortalShowcase exposes only curated names and built-in icon IDs.
func (h *Handler) GetPublicPortalShowcase(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"items": h.publicShowcaseItems()})
}

func canManagePortalShowcase(c *gin.Context) bool {
	p, ok := principalFromContext(c)
	if !ok || (!p.PlatformAdmin && p.EffectiveTenant.ID != identity.SystemTenantID) {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "system tenant configuration required"})
		return false
	}
	return true
}

// GetPortalShowcase returns the saved display list and allowed icon choices.
func (h *Handler) GetPortalShowcase(c *gin.Context) {
	if !canManagePortalShowcase(c) {
		return
	}
	cfg := h.freshConfig(identity.SystemTenantID)
	setVersionHeader(c, cfg.RuntimeSettingState().Version(runtimeconfig.RuntimeSettingPortalShowcase))
	items := cfg.PortalModelShowcase
	if items == nil {
		items = []config.PortalShowcaseModel{}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"items": items, "available_icons": portalShowcaseIcons, "version": cfg.RuntimeSettingState().Version(runtimeconfig.RuntimeSettingPortalShowcase)})
}

// PutPortalShowcase replaces the public display list through the existing
// versioned settings store. Validation completes before any state is changed.
func (h *Handler) PutPortalShowcase(c *gin.Context) {
	if !canManagePortalShowcase(c) {
		return
	}
	body, err := bodyutil.GetRequestBodyStorage(c, 16384)
	if err != nil || body == nil {
		status := http.StatusBadRequest
		if bodyutil.IsTooLarge(err) {
			status = http.StatusRequestEntityTooLarge
		}
		c.AbortWithStatusJSON(status, gin.H{"error": "invalid request body"})
		return
	}
	var payload struct {
		Items   *[]config.PortalShowcaseModel `json:"items"`
		Version *int64                        `json:"version,omitempty"`
	}
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || payload.Items == nil || len(*payload.Items) > 50 || (payload.Version != nil && *payload.Version < 0) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "items must be an array of at most 50 models"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	icons := make(map[string]bool, len(portalShowcaseIcons))
	for _, icon := range portalShowcaseIcons {
		icons[icon] = true
	}
	seen := make(map[string]bool)
	items := make([]config.PortalShowcaseModel, 0, len(*payload.Items))
	for _, item := range *payload.Items {
		item.Name = strings.TrimSpace(item.Name)
		item.Icon = strings.ToLower(strings.TrimSpace(item.Icon))
		key := strings.ToLower(item.Name)
		if item.Name == "" || !utf8.ValidString(item.Name) || utf8.RuneCountInString(item.Name) > 80 || strings.ContainsAny(item.Name, "\r\n\t") || !icons[item.Icon] || seen[key] {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "use unique names of 1–80 characters and a built-in icon"})
			return
		}
		seen[key] = true
		items = append(items, item)
	}
	h.mutateSystemConfig(c, func(cfg *config.Config) error { cfg.PortalModelShowcase = items; return nil })
}
