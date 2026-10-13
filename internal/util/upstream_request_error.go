package util

import sdkexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"

// IsUpstreamRequestBufferLimit identifies the observed request-specific 507.
// Other 507 failures retain the provider's ordinary recovery policy.
func IsUpstreamRequestBufferLimit(err error) bool {
	return sdkexecutor.IsUpstreamRequestBufferLimit(err)
}
