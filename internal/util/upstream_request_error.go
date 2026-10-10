package util

import "strings"

// IsUpstreamRequestBufferLimit identifies the observed request-specific 507.
// Other 507 failures retain the provider's ordinary recovery policy.
func IsUpstreamRequestBufferLimit(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "exceeded request buffer limit while retrying upstream")
}
