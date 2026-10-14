package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

func TestWriteStreamKeepAliveRestoresTransportFlag(t *testing.T) {
	for _, panics := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		func() {
			defer func() {
				if recovered := recover(); (recovered != nil) != panics {
					t.Fatalf("unexpected panic %v", recovered)
				}
			}()
			WriteStreamKeepAlive(c, func() {
				if value, _ := c.Get(requestctx.GinKeyTransportHeartbeat); value != true {
					t.Fatalf("heartbeat flag=%v", value)
				}
				if panics {
					panic("injected write panic")
				}
			})
		}()
		if value, _ := c.Get(requestctx.GinKeyTransportHeartbeat); value != false {
			t.Fatalf("transport flag not restored: %v", value)
		}
	}
}

type startupForbiddenWriter struct{ gin.ResponseWriter }

func (startupForbiddenWriter) Written() bool { panic("background worker accessed HTTP writer") }

func TestAsyncModelResolutionDefersWriterAccess(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	realWriter := c.Writer
	c.Writer = startupForbiddenWriter{c.Writer}
	c.Set(requestctx.GinKeyAsyncResponseStatus, 200)
	RememberRequestModelResolution(c, RequestModelResolution{RequestedModel: "original", Model: "target", Reason: "model_unavailable"})
	c.Writer = realWriter
	ApplyRequestModelResolutionHeaders(c)
	if got := c.Writer.Header().Get("X-Clirelay-Resolved-Model"); got != "target" {
		t.Fatalf("resolution header=%q", got)
	}
}
