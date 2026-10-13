// Package diagnostics exposes request diagnostic context to the SDK boundary.
package diagnostics

import (
	"context"
	"github.com/gin-gonic/gin"
	internaldiagnostics "github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
)

type RequestDiagnostic = internaldiagnostics.RequestDiagnostic

func FromContext(ctx context.Context) *RequestDiagnostic { return internaldiagnostics.FromContext(ctx) }
func WithContext(ctx context.Context, d *RequestDiagnostic) context.Context {
	return internaldiagnostics.WithContext(ctx, d)
}
func EnsureGin(c *gin.Context, id string) *RequestDiagnostic {
	return internaldiagnostics.EnsureGin(c, id)
}
