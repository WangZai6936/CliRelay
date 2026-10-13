// Package logging exposes application request correlation to SDK handlers.
package logging

import (
	"github.com/gin-gonic/gin"
	app "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
)

// RenewGinRequestCorrelationID starts a logical generation on an existing
// WebSocket while retaining the handshake's human-readable request ID.
func RenewGinRequestCorrelationID(c *gin.Context) { app.RenewGinRequestCorrelationID(c) }
