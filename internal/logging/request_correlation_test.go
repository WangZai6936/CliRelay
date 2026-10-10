package logging

import (
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"testing"
)

func TestPrivateRequestCorrelationRotatesPerWebsocketTurn(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	SetGinRequestID(c, "compact-id")
	first := GetGinRequestCorrelationID(c)
	SetGinRequestID(c, "compact-id")
	if len(first) != 32 || GetGinRequestCorrelationID(c) != first {
		t.Fatal("HTTP request identity is not stable")
	}
	RenewGinRequestCorrelationID(c)
	if GetGinRequestCorrelationID(c) == first || len(GetGinRequestCorrelationID(c)) != 32 || GetGinRequestID(c) != "compact-id" {
		t.Fatal("WebSocket turn did not receive a separate identity")
	}
}
