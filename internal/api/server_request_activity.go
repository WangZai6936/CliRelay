package api

import (
	"net/http"
	"strings"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// trackActiveAPIRequests counts request handlers on this server, before body
// decompression, logging, auth and quota admission. A stream remains active
// until its handler returns, including an idle Responses WebSocket handler.
// Detached provider relay sessions are not API requests; requests using them
// are counted at their own HTTP entry point.
func (s *Server) trackActiveAPIRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isActiveAPIRequestPath(r.URL.Path) {
			s.activeAPIRequests.Add(1)
			defer s.activeAPIRequests.Add(-1)
			r = r.WithContext(coreusage.BeginRequest(r.Context()))
			defer coreusage.FinishRequest(r.Context())
		}
		next.ServeHTTP(w, r)
	})
}

// Management polling must not count itself, otherwise a drain check could
// never observe zero. All other paths are counted conservatively, including
// group-prefixed and module-provided inference routes.
func isActiveAPIRequestPath(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/management.html", "/manage", "/management", "/v0/management":
		return false
	}
	return !strings.HasPrefix(path, "/readyz/") &&
		!strings.HasPrefix(path, "/manage/") &&
		!strings.HasPrefix(path, "/management/") &&
		!strings.HasPrefix(path, "/v0/management/")
}
