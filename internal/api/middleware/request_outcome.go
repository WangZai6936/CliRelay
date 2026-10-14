package middleware

import (
	"bytes"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
	requestusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type terminalErrorWriter struct {
	gin.ResponseWriter
	body bytes.Buffer
}

func (w *terminalErrorWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *terminalErrorWriter) capture(p []byte) {
	if w.Status() >= 400 && w.body.Len() < 8192 {
		n := min(len(p), 8192-w.body.Len())
		_, _ = w.body.Write(p[:n])
	}
}
func (w *terminalErrorWriter) Write(p []byte) (int, error) {
	w.capture(p)
	return w.ResponseWriter.Write(p)
}
func (w *terminalErrorWriter) WriteString(p string) (int, error) {
	w.capture([]byte(p))
	return w.ResponseWriter.WriteString(p)
}

// RequestOutcomeMiddleware captures admission errors even when no executor ran.
// Only classified codes survive; the bounded temporary JSON body is discarded.
func RequestOutcomeMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		if requestusage.RequestIdentity(ctx) == "" {
			c.Next()
			return
		}
		requestusage.ObserveRequestPhase(ctx, "admission")
		writer := &terminalErrorWriter{ResponseWriter: c.Writer}
		c.Writer = writer
		c.Next()
		requestusage.ObserveRequestHTTPStatus(ctx, c.Writer.Status())
		record := requestusage.Record{APIKey: c.GetString("apiKey"), TrustedTenantID: strings.TrimSpace(c.GetString("tenantID"))}
		if d := diagnostics.FromContext(ctx); d != nil {
			snapshot := d.Snapshot()
			record.RequestID = snapshot.RequestID
			if snapshot.Auth != nil {
				record.APIKeyID = snapshot.Auth.APIKeyID
				record.APIKeyName = snapshot.Auth.APIKeyName
			}
			if snapshot.Body != nil {
				record.Model = snapshot.Body.Model
			}
		}
		requestusage.ObserveRequestIdentity(ctx, record)
		if c.Writer.Status() >= 400 {
			requestusage.ObserveRequestError(ctx, c.Writer.Status(), errors.New(writer.body.String()))
			requestusage.MarkRequestFailed(ctx)
		}
	}
}
