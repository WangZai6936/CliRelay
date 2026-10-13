package executor

import (
	"context"
	"hash/fnv"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/tlsfingerprint"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// The request ID (independent of account/model/result) makes every retry stay
// in the same arm. There is no account-affinity that would bias the comparison.
func codexHTTP1Cohort(requestID string, anonymousSequence uint64, percent int) bool {
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	var bucket uint64
	if requestID != "" {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(requestID))
		bucket = hash.Sum64() % 100
	} else {
		bucket = anonymousSequence % 100
	}
	return bucket < uint64(percent)
}

// applyCodexHTTPPolicy selects an existing process-wide connection pool. It
// never silently replaces an externally supplied custom RoundTripper: that
// transport may implement egress requirements the standard pool cannot honor.
func applyCodexHTTPPolicy(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, proxyURL string, base http.RoundTripper) (http.RoundTripper, string, string) {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return base, "", ""
	}
	kind := "custom"
	switch base.(type) {
	case *http.Transport:
		kind = "standard"
	case *tlsfingerprint.RoundTripper:
		kind = "tls_fingerprint"
	}
	percent := 0
	if cfg != nil {
		percent = cfg.CodexHTTPTransport.CanaryPercent()
	}
	requestID := coreusage.RequestIdentity(ctx)
	if requestID == "" {
		requestID = logging.GetRequestID(ctx)
	}
	if diagnostic := diagnostics.FromContext(ctx); diagnostic != nil && requestID == "" {
		requestID = diagnostic.Snapshot().RequestID
	}
	if requestID == "" {
		// An internal caller with no logical identity cannot participate in a
		// stable request-level experiment. Do not randomize individual retries.
		return base, "unassigned_default", kind
	}
	if !codexHTTP1Cohort(requestID, 0, percent) {
		return base, "default", kind
	}
	switch kind {
	case "tls_fingerprint":
		if rt := tlsFingerprintProtocolRoundTripper(cfg, auth, proxyURL, true); rt != nil {
			return rt, "http1_canary", kind
		}
		return base, "http1_canary_unavailable", kind
	case "standard":
		// A context-owned *http.Transport can contain custom dialers. Only
		// select the shared HTTP/1 pool when this is our known cached pool.
		known := cachedDirectTransport(cfgToSDKCfg(cfg))
		if proxyURL != "" {
			known = cachedProxyTransport(proxyURL, cfgToSDKCfg(cfg))
		}
		if base == known {
			if rt := sharedUpstreamTransportCache.getProtocol(proxyURL, cfgToSDKCfg(cfg), proxyURL == "", true); rt != nil {
				return rt, "http1_canary", kind
			}
		}
	}
	return base, "http1_canary_unsupported", kind
}
