package executor

import (
	"context"
	"strings"

	"github.com/google/uuid"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// The upstream requires a conversation ID. Forward the client's explicit
// X-Opencode-Session when present; otherwise synthesize one from other session
// signals on the request, or a per-request UUID. Neither a shared credential ID
// nor a bare request ID identifies a conversation.
func opencodeGoAuthWithSessionHeader(ctx context.Context, auth *cliproxyauth.Auth, opts cliproxyexecutor.Options) *cliproxyauth.Auth {
	session := opencodeGoExplicitOpencodeSession(ctx, opts)
	if session == "" {
		session = opencodeGoSynthesizeOpencodeSession(ctx, opts)
	}
	if auth == nil || session == "" {
		return auth
	}
	// Credentials are shared across concurrent requests; request headers belong
	// only to this execution and must never be persisted onto the account.
	scoped := auth.Clone()
	if scoped.Attributes == nil {
		scoped.Attributes = make(map[string]string)
	}
	scoped.Attributes["header:X-Opencode-Session"] = session
	return scoped
}

func opencodeGoExplicitOpencodeSession(ctx context.Context, opts cliproxyexecutor.Options) string {
	session := strings.TrimSpace(opts.Headers.Get("X-Opencode-Session"))
	if session == "" {
		if ginCtx := ginContextFrom(ctx); ginCtx != nil && ginCtx.Request != nil {
			session = strings.TrimSpace(ginCtx.Request.Header.Get("X-Opencode-Session"))
		}
	}
	return session
}

// Mirrors sdk/api/handlers/session_sticky.go header precedence for stable
// client conversation identity when X-Opencode-Session is absent.
var opencodeGoAlternateSessionHeaderNames = []string{
	"Session-Id",
	"session_id",
	"X-Session-Id",
	"X-Codex-Session-Id",
	"X-Claude-Code-Session-Id",
	"X-Grok-Session-Id",
	"X-Deepseek-Harness-Session-Id",
	"Conversation-Id",
	"conversation_id",
	"X-Conversation-Id",
	"OpenAI-Conversation-Id",
	"X-Grok-Conv-Id",
}

func opencodeGoSynthesizeOpencodeSession(ctx context.Context, opts cliproxyexecutor.Options) string {
	if opts.Headers != nil {
		if session := opencodeGoSessionFromHTTPHeader(opts.Headers); session != "" {
			return session
		}
	}
	if ginCtx := ginContextFrom(ctx); ginCtx != nil && ginCtx.Request != nil {
		if session := opencodeGoSessionFromHTTPHeader(ginCtx.Request.Header); session != "" {
			return session
		}
	}
	if raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]; ok {
		if s, ok := raw.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	if raw, ok := opts.Metadata[cliproxyexecutor.SessionStickyMetadataKey]; ok {
		if s, ok := raw.(string); ok {
			if session := opencodeGoSessionFromStickyMetadata(s); session != "" {
				return session
			}
		}
	}
	return uuid.NewString()
}

func opencodeGoSessionFromHTTPHeader(h interface{ Get(string) string }) string {
	for _, name := range opencodeGoAlternateSessionHeaderNames {
		if value := strings.TrimSpace(h.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func opencodeGoSessionFromStickyMetadata(sticky string) string {
	sticky = strings.TrimSpace(sticky)
	// Sticky keys use header:<lowercase-name>:<value> (see requestSessionStickyHeaderKey).
	parts := strings.SplitN(sticky, ":", 3)
	if len(parts) != 3 || parts[0] != "header" {
		return ""
	}
	return strings.TrimSpace(parts[2])
}
