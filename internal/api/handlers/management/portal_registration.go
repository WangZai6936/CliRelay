package management

import (
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/bodyutil"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/enduser"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	log "github.com/sirupsen/logrus"
)

//go:embed portal_registration.html
var portalRegistrationHTML []byte

func (h *Handler) portalRegistrationConfig() config.PortalRegistrationConfig {
	if h == nil {
		return config.PortalRegistrationConfig{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return config.PortalRegistrationConfig{}
	}
	return h.cfg.PortalRegistration
}

// GetPortalRegistration exposes only the public form capabilities.
func (h *Handler) GetPortalRegistration(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"enabled":             h.portalRegistrationConfig().Enabled,
		"password_min_length": identity.PasswordMinLength,
		"password_max_bytes":  identity.PasswordMaxBytes,
	})
}

// GetPortalRegistrationPage serves a standalone form without depending on a
// separately released management panel version.
func (h *Handler) GetPortalRegistrationPage(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	c.Header("X-Frame-Options", "DENY")
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Referrer-Policy", "no-referrer")
	c.Data(http.StatusOK, "text/html; charset=utf-8", portalRegistrationHTML)
}

// PostPortalRegister accepts only account credentials. Tenant, status, roles,
// quota and permission profiles are determined by the server.
func (h *Handler) PostPortalRegister(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	policy := h.portalRegistrationConfig()
	if !policy.Enabled {
		c.JSON(http.StatusForbidden, gin.H{"error": gin.H{"code": "registration_disabled", "message": "public registration is disabled"}})
		return
	}
	// Charge every attempt before parsing or bcrypt, independently of login
	// guessing budgets; successful signups must not reset this bucket.
	key := h.clientThrottleKey(c, scopePortalRegistration)
	if decision := h.throttleCharge(c, key, time.Now()); decision.Outcome != outcomeAllow {
		abortThrottled(c, decision)
		return
	}
	svc := h.endUserService()
	if svc == nil {
		portalRegistrationUnavailable(c)
		return
	}
	var body struct {
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		Password    string `json:"password"`
	}
	// The general management body middleware skips multipart uploads. Enforce
	// the signup limit here as well, regardless of Content-Type or transfer mode.
	storage, err := bodyutil.GetRequestBodyStorage(c, 4096)
	if err != nil {
		status := http.StatusBadRequest
		if bodyutil.IsTooLarge(err) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": gin.H{"code": "validation_failed", "message": "invalid registration body"}})
		return
	}
	if storage == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "validation_failed", "message": "invalid registration body"}})
		return
	}
	decoder := json.NewDecoder(storage)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "validation_failed", "message": "invalid registration body"}})
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "validation_failed", "message": "invalid registration body"}})
		return
	}
	tenantID := strings.TrimSpace(policy.TenantID)
	if tenantID == "" {
		tenantID = identity.SystemTenantID
	}
	user, err := svc.Register(c.Request.Context(), enduser.RegistrationOptions{
		TenantID: tenantID, PermissionProfileID: policy.PermissionProfileID,
	}, body.Username, body.DisplayName, body.Password)
	if err != nil {
		switch {
		case errors.Is(err, enduser.ErrUsernameTaken):
			c.JSON(http.StatusConflict, gin.H{"error": gin.H{"code": "username_taken", "message": "username already taken"}})
		case errors.Is(err, enduser.ErrValidation):
			endUserError(c, err)
		default:
			log.WithError(err).Error("portal registration failed")
			portalRegistrationUnavailable(c)
		}
		return
	}
	if err = h.refreshAPIKeyCache(); err != nil {
		// The account is already committed. Returning a failure here would
		// invite a retry for an account that has successfully been created.
		log.WithError(err).Warn("portal registration: refresh API key cache")
	}
	c.JSON(http.StatusCreated, gin.H{"user": user})
}

func portalRegistrationUnavailable(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"code": "registration_unavailable", "message": "registration temporarily unavailable"}})
}
