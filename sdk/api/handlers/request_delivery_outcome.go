package handlers

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	requestusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdkbridge/diagnostics"
)

// deliveryOutcomeWriter preserves Gin's optional writer interfaces while
// recording write failures which protocol handlers historically ignored.
type deliveryOutcomeWriter struct {
	gin.ResponseWriter
	ctx context.Context
}

func (w *deliveryOutcomeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *deliveryOutcomeWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if err != nil {
		requestusage.MarkRequestDeliveryFailed(w.ctx)
	}
	return n, err
}

func (w *deliveryOutcomeWriter) WriteString(s string) (int, error) {
	n, err := w.ResponseWriter.WriteString(s)
	if err != nil {
		requestusage.MarkRequestDeliveryFailed(w.ctx)
	}
	return n, err
}

func observeRequestDelivery(c *gin.Context, ctx context.Context) {
	observeAuthenticatedRequestIdentity(c, ctx, "")
	if c == nil || c.Writer == nil {
		return
	}
	if w, ok := c.Writer.(*deliveryOutcomeWriter); ok {
		w.ctx = ctx
		return
	}
	c.Writer = &deliveryOutcomeWriter{ResponseWriter: c.Writer, ctx: ctx}
}

// Snapshot authenticated Gin identity before an executor can fail or the
// client can disconnect. Neither usage publication nor its cancellation
// reporter is guaranteed to run before the HTTP handler returns.
func observeAuthenticatedRequestIdentity(c *gin.Context, ctx context.Context, model string) {
	if c == nil || ctx == nil {
		return
	}
	record := requestusage.Record{APIKey: c.GetString("apiKey"), TrustedTenantID: strings.TrimSpace(c.GetString("tenantID")), Model: model}
	if diagnostic := diagnostics.FromContext(ctx); diagnostic != nil {
		snapshot := diagnostic.Snapshot()
		record.RequestID = snapshot.RequestID
		if snapshot.Auth != nil {
			record.APIKeyID, record.APIKeyName = snapshot.Auth.APIKeyID, snapshot.Auth.APIKeyName
		}
		if record.Model == "" && snapshot.Body != nil {
			record.Model = snapshot.Body.Model
		}
	}
	requestusage.ObserveRequestIdentity(ctx, record)
}

// The callback runs before the execution context is intentionally canceled.
// Only the original HTTP context indicates client cancellation; canceling the
// child context after a successful response is routine resource cleanup.
func markRequestDeliveryOutcome(c *gin.Context, ctx context.Context, params ...interface{}) {
	if c != nil && c.Request != nil && c.Request.Context().Err() != nil {
		requestusage.MarkRequestCanceled(ctx)
		return
	}
	for _, param := range params {
		if err, ok := param.(error); ok && err != nil {
			requestusage.MarkRequestFailedWithError(ctx, err)
			return
		}
	}
	requestusage.MarkRequestSucceeded(ctx)
}

func (h *BaseAPIHandler) requestDeliveryCancel(c *gin.Context, newCtx context.Context, cancel context.CancelFunc) APIHandlerCancelFunc {
	return func(params ...interface{}) {
		markRequestDeliveryOutcome(c, newCtx, params...)
		if h.Cfg.RequestLog && len(params) == 1 {
			if existing, exists := c.Get("API_RESPONSE"); exists {
				if existingBytes, ok := existing.([]byte); ok && len(bytes.TrimSpace(existingBytes)) > 0 {
					switch params[0].(type) {
					case error, string:
						cancel()
						return
					}
				}
			}

			var payload []byte
			switch data := params[0].(type) {
			case []byte:
				payload = data
			case error:
				if data != nil {
					payload = []byte(data.Error())
				}
			case string:
				payload = []byte(data)
			}
			if len(payload) > 0 {
				if existing, exists := c.Get("API_RESPONSE"); exists {
					if existingBytes, ok := existing.([]byte); ok && len(existingBytes) > 0 {
						trimmedPayload := bytes.TrimSpace(payload)
						if len(trimmedPayload) > 0 && bytes.Contains(existingBytes, trimmedPayload) {
							cancel()
							return
						}
					}
				}
				appendAPIResponse(c, payload)
			}
		}

		cancel()
	}
}

func inheritRequestDiagnostics(parentCtx, requestCtx context.Context, c *gin.Context, id string) context.Context {
	if diagnostics.FromContext(parentCtx) != nil {
		return parentCtx
	}
	diagnostic := diagnostics.FromContext(requestCtx)
	if diagnostic == nil && c != nil {
		diagnostic = diagnostics.EnsureGin(c, id)
	}
	return diagnostics.WithContext(parentCtx, diagnostic)
}
