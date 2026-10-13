package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/tlsfingerprint"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"golang.org/x/net/http2"
)

func TestCodexHTTP1CohortStableAndBounded(t *testing.T) {
	canary := 0
	for i := range 10000 {
		id := fmt.Sprintf("request-%d", i)
		arm := codexHTTP1Cohort(id, 0, 5)
		if arm != codexHTTP1Cohort(id, 99, 5) {
			t.Fatal("retry changed group")
		}
		if arm {
			canary++
		}
		if codexHTTP1Cohort(id, 0, 0) || !codexHTTP1Cohort(id, 0, 100) {
			t.Fatal("invalid percentage endpoint")
		}
	}
	if canary < 400 || canary > 600 {
		t.Fatalf("5%% cohort contains %d of 10000 requests", canary)
	}
}

// Both arms make real TLS requests to the same dual-protocol server. This
// prevents a config-only test passing while utls continues to negotiate h2.
func TestCodexProtocolComparisonRealConnections(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "mock-request")
		w.Header().Set("Cf-Ray", "mock-ray")
		w.Header().Set("X-Protocol", r.Proto)
		_, _ = io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	for _, fingerprint := range []bool{false, true} {
		for _, percent := range []int{0, 100} {
			t.Run(fmt.Sprintf("fingerprint-%t-percent-%d", fingerprint, percent), func(t *testing.T) {
				cfg := &config.Config{CodexHTTPTransport: config.CodexHTTPTransportConfig{HTTP1Percent: percent}}
				cfg.InsecureSkipVerify = true
				cfg.IdentityFingerprint.Codex.TLSFingerprint = config.TLSFingerprintConfig{Enabled: fingerprint, Profile: "chrome"}
				auth := &cliproxyauth.Auth{Provider: "codex"}
				want := "HTTP/2.0"
				group := "default"
				if percent == 100 {
					want, group = "HTTP/1.1", "http1_canary"
				}
				connectionID := ""
				for i := range 2 {
					d := &diagnostics.RequestDiagnostic{RequestID: "same-logical-request"}
					ctx := diagnostics.WithContext(context.Background(), d)
					diagnostics.SetUpstreamRequest(ctx, i+1, "codex", "mock", "", server.URL, "GET")
					client := newProxyAwareHTTPClient(ctx, cfg, auth, 0)
					request, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
					resp, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					_, readErr := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					if readErr != nil {
						t.Fatal(readErr)
					}
					u := d.Snapshot().Upstream
					if resp.Proto != want || resp.Header.Get("X-Protocol") != want || u.Protocol != want || u.TransportGroup != group {
						t.Fatalf("protocol did not take effect: response=%q upstream=%+v", resp.Proto, u)
					}
					if u.RequestID != "mock-request" || u.CFRay != "mock-ray" || u.BodyBytes != 2 || u.ResponseHeadersAt == "" || u.FirstBodyAt == "" {
						t.Fatalf("missing diagnostics: %+v", u)
					}
					if len(u.ConnectionID) != 24 || u.ConnectionReused == nil {
						t.Fatalf("missing connection diagnostic: %+v", u)
					}
					if i == 1 && (connectionID != u.ConnectionID || !*u.ConnectionReused) {
						t.Fatalf("second request did not reuse first connection: %+v", u)
					}
					connectionID = u.ConnectionID
				}
			})
		}
	}
}

func TestCodexHTTP1OnlyChangesKnownCodexTransport(t *testing.T) {
	cfg := &config.Config{CodexHTTPTransport: config.CodexHTTPTransportConfig{HTTP1Percent: 100}}
	base := cachedDirectTransport(&cfg.SDKConfig)
	for _, provider := range []string{"claude", "openai", "gemini"} {
		rt, group, _ := applyCodexHTTPPolicy(context.Background(), cfg, &cliproxyauth.Auth{Provider: provider}, "", base)
		if rt != base || group != "" {
			t.Fatalf("changed %s transport", provider)
		}
	}
	custom := &http.Transport{}
	ctx := sdkexecutor.WithRoundTripper(logging.WithRequestID(context.Background(), "custom-egress-request"), custom)
	client := newProxyAwareHTTPClient(ctx, cfg, &cliproxyauth.Auth{Provider: "codex"}, 0)
	tracing := client.Transport.(*upstreamTracingTransport)
	if tracing.base != custom || tracing.transportGroup != "http1_canary_unsupported" {
		t.Fatal("overrode custom egress")
	}
	first := tlsFingerprintProtocolRoundTripper(&config.Config{IdentityFingerprint: config.IdentityFingerprintConfig{Codex: config.CodexIdentityFingerprintConfig{TLSFingerprint: config.TLSFingerprintConfig{Enabled: true}}}}, &cliproxyauth.Auth{Provider: "codex"}, "", true)
	second := tlsFingerprintProtocolRoundTripper(&config.Config{IdentityFingerprint: config.IdentityFingerprintConfig{Codex: config.CodexIdentityFingerprintConfig{TLSFingerprint: config.TLSFingerprintConfig{Enabled: true}}}}, &cliproxyauth.Auth{Provider: "codex"}, "", true)
	if first == nil || first != second {
		t.Fatal("created per-request fingerprint pools")
	}
	if _, ok := first.(*tlsfingerprint.RoundTripper); !ok {
		t.Fatal("lost TLS fingerprint transport")
	}
}

func TestCodexProtocolCohortUsesLogicalIdentityWithoutDiagnostics(t *testing.T) {
	cfg := &config.Config{CodexHTTPTransport: config.CodexHTTPTransportConfig{HTTP1Percent: 5}}
	auth := &cliproxyauth.Auth{Provider: "codex"}
	base := cachedDirectTransport(&cfg.SDKConfig)
	logical := coreusage.BeginRequest(context.Background())
	want := "default"
	if codexHTTP1Cohort(coreusage.RequestIdentity(logical), 0, 5) {
		want = "http1_canary"
	}
	for i := range 100 {
		// Compact log IDs can change without changing the logical request.
		ctx := logging.WithRequestID(logical, fmt.Sprintf("different-attempt-%d", i))
		_, group, _ := applyCodexHTTPPolicy(ctx, cfg, auth, "", base)
		if group != want {
			t.Fatalf("logical request crossed experiment groups: %s want %s", group, want)
		}
	}
	for range 100 {
		rt, group, _ := applyCodexHTTPPolicy(context.Background(), cfg, auth, "", base)
		if rt != base || group != "unassigned_default" {
			t.Fatal("anonymous attempt was randomized into a request cohort")
		}
	}
	cfg.CodexHTTPTransport.HTTP1Percent = 100
	_, group, _ := applyCodexHTTPPolicy(logging.WithRequestID(context.Background(), "sdk-request"), cfg, auth, "", base)
	if group != "http1_canary" {
		t.Fatal("logging request identity fallback was ignored")
	}
}

func TestCompactUpstreamUsageDiagnostics(t *testing.T) {
	reused := true
	d := &diagnostics.RequestDiagnostic{Upstream: &diagnostics.UpstreamSnapshot{Protocol: "HTTP/1.1", TransportGroup: "http1_canary", TransportKind: "tls_fingerprint", ConnectionID: "anonymous-connection", ConnectionReused: &reused, ErrorStage: "after_output"}}
	ctx := diagnostics.WithContext(context.Background(), d)
	var record coreusage.Record
	attachUpstreamUsageDiagnostics(ctx, &record)
	if record.UpstreamProtocol != "HTTP/1.1" || record.UpstreamTransportGroup != "http1_canary" || record.UpstreamTransportKind != "tls_fingerprint" || record.UpstreamConnectionID != "anonymous-connection" || record.UpstreamConnectionReused == nil || !*record.UpstreamConnectionReused || record.UpstreamErrorStage != "after_output" {
		t.Fatalf("missing compact record diagnostics: %+v", record)
	}
	if record.InputContent != "" || record.OutputContent != "" || record.DetailContent != "" {
		t.Fatal("compact protocol metadata retained bodies")
	}
	*record.UpstreamConnectionReused = false
	if !*d.Snapshot().Upstream.ConnectionReused {
		t.Fatal("usage metadata retained mutable diagnostic pointer")
	}
}

func TestCodexHTTP2ResetDiagnosticsPreserveOutputStage(t *testing.T) {
	clientReadFirstBody := make(chan struct{})
	release := sync.OnceFunc(func() { close(clientReadFirstBody) })
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "reset-fixture")
		_, _ = io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		// Flush only queues a frame; the peer must confirm it has received
		// headers and the complete first body chunk before we emit the RST.
		select {
		case <-clientReadFirstBody:
			panic(http.ErrAbortHandler)
		case <-r.Context().Done():
			return
		}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	defer release()
	// Each fixture needs its own pool. The process-wide fingerprint cache can
	// still hold an older httptest connection for 127.0.0.1 on another port
	// while its close/GOAWAY is being processed, before this server is dialed.
	rt, err := tlsfingerprint.New(tlsfingerprint.Options{Profile: "chrome", InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.CloseIdleConnections()
	cfg := &config.Config{}
	d := &diagnostics.RequestDiagnostic{RequestID: "reset-stage"}
	ctx := diagnostics.WithContext(context.Background(), d)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = sdkexecutor.WithRoundTripper(ctx, rt)
	diagnostics.SetUpstreamRequest(ctx, 1, "codex", "mock", "", server.URL, "GET")
	client := newProxyAwareHTTPClient(ctx, cfg, &cliproxyauth.Auth{Provider: "codex"}, 0)
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	if string(first) != "first" || resp.Proto != "HTTP/2.0" {
		t.Fatalf("first frame = %q protocol = %s", first, resp.Proto)
	}
	diagnostics.RecordUpstreamOutput(ctx, 5)
	release()
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var reset http2.StreamError
	if !errors.As(err, &reset) || reset.Code != http2.ErrCodeInternal {
		t.Fatalf("expected real HTTP/2 INTERNAL_ERROR stream reset, got %T: %v", err, err)
	}
	u := d.Snapshot().Upstream
	if u.Protocol != "HTTP/2.0" || u.ErrorStage != "after_output" || u.ErrorAt == "" || u.OutputBytes != 5 || u.RequestID != "reset-fixture" {
		t.Fatalf("incomplete reset diagnostic: %+v", u)
	}
}
