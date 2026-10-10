package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/bodyutil"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// rewriteRequestModelAlias runs before catalog/permission checks and payload
// translation. It changes only model; tools and conversation remain intact.
func (s *Server) rewriteRequestModelAlias(c *gin.Context) bool {
	if s.cfg == nil || len(s.cfg.RequestModelAliases) == 0 || c.Request.Method != http.MethodPost || !strings.Contains(c.GetHeader("Content-Type"), "application/json") {
		return true
	}
	body, err := bodyutil.ReadRequestBody(c, bodyutil.ModelRequestBodyLimit())
	if err != nil {
		if bodyutil.IsTooLarge(err) {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body too large"})
			return false
		}
		if bodyutil.IsStorageUnavailable(err) {
			c.AbortWithStatusJSON(http.StatusInsufficientStorage, gin.H{"error": "request body temporary storage unavailable"})
			return false
		}
		return true
	}
	if !gjson.ValidBytes(body) {
		return true
	}
	model := gjson.GetBytes(body, "model").String()
	target := s.cfg.ResolveRequestModelAlias(model)
	if target == model || model == "" {
		return true
	}
	rewritten, err := sjson.SetBytes(body, "model", target)
	if err != nil {
		return true
	}
	bodyutil.SetRequestBody(c, rewritten)
	c.Header("X-Clirelay-Resolved-Model", target)
	return true
}
