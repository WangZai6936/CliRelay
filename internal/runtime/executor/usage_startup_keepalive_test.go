package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

type startupUnsafeStatusWriter struct{ gin.ResponseWriter }

func (startupUnsafeStatusWriter) Status() int { panic("worker accessed live HTTP writer") }

func TestUsageRequestMetadataUsesAsyncResponseStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Writer = startupUnsafeStatusWriter{c.Writer}
	c.Set(requestctx.GinKeyAsyncResponseStatus, http.StatusOK)
	ctx := context.WithValue(c.Request.Context(), requestctx.ContextKeyGin, c)
	identifier, _, status := usageRequestMetadata(ctx)
	if identifier != "POST /v1/chat/completions" || status != http.StatusOK {
		t.Fatalf("identifier=%q status=%d", identifier, status)
	}
}
