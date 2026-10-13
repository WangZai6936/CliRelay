package executor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/diagnostics"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
)

type upstreamTraceTimings struct {
	mu            sync.Mutex
	Host          string `json:"host,omitempty"`
	Scheme        string `json:"scheme,omitempty"`
	GotConnReused bool   `json:"got_conn_reused"`
	WasIdle       bool   `json:"was_idle,omitempty"`
	IdleMs        int64  `json:"idle_ms,omitempty"`
	ConnectMs     int64  `json:"connect_ms,omitempty"`
	TLSMs         int64  `json:"tls_ms,omitempty"`
	HeaderTTFBMs  int64  `json:"header_ttfb_ms,omitempty"`

	getConnAt              time.Time
	gotConnAt              time.Time
	connectStartAt         time.Time
	connectDoneAt          time.Time
	tlsHandshakeStartAt    time.Time
	tlsHandshakeDoneAt     time.Time
	gotFirstResponseByteAt time.Time
	requestStart           time.Time
	diagnosticContext      context.Context
}

const upstreamTimingKey = "cliproxy.request_log.upstream_timing"

func (t *upstreamTraceTimings) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.getConnAt = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.gotConnAt = time.Now()
			t.GotConnReused = info.Reused
			t.WasIdle = info.WasIdle
			if info.Conn != nil {
				diagnostics.RecordUpstreamConnection(t.diagnosticContext, upstreamConnectionID(info.Conn), info.Reused)
			}
			if info.WasIdle {
				t.IdleMs = info.IdleTime.Milliseconds()
			}
		},
		ConnectStart: func(_, _ string) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.connectStartAt = time.Now()
		},
		ConnectDone: func(_, _ string, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.connectDoneAt = time.Now()
			if err == nil && !t.connectStartAt.IsZero() {
				t.ConnectMs = t.connectDoneAt.Sub(t.connectStartAt).Milliseconds()
			}
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsHandshakeStartAt = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsHandshakeDoneAt = time.Now()
			if err == nil && !t.tlsHandshakeStartAt.IsZero() {
				t.TLSMs = t.tlsHandshakeDoneAt.Sub(t.tlsHandshakeStartAt).Milliseconds()
			}
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.gotFirstResponseByteAt = time.Now()
			if !t.gotConnAt.IsZero() {
				t.HeaderTTFBMs = t.gotFirstResponseByteAt.Sub(t.gotConnAt).Milliseconds()
			}
		},
	}
}

type upstreamTracingTransport struct {
	base           http.RoundTripper
	ginCtx         *gin.Context
	transportGroup string
	transportKind  string
}

func (t *upstreamTracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ginCtx := t.ginCtx
	if ginCtx == nil {
		ginCtx, _ = req.Context().Value(util.ContextKeyGin).(*gin.Context)
	}

	trace := &upstreamTraceTimings{requestStart: time.Now(), diagnosticContext: req.Context()}
	if t.transportGroup != "" {
		diagnostics.RecordUpstreamTransport(req.Context(), t.transportGroup, t.transportKind)
	}
	if req.URL != nil {
		trace.Host = req.URL.Host
		trace.Scheme = req.URL.Scheme
	}

	traceCtx := httptrace.WithClientTrace(req.Context(), trace.clientTrace())
	reqWithTrace := req.WithContext(traceCtx)

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(reqWithTrace)
	if err != nil && t.transportGroup != "" {
		diagnostics.SetUpstreamError(req.Context(), err)
	}
	if resp != nil && t.transportGroup != "" {
		diagnostics.RecordUpstreamHTTPResponse(req.Context(), resp.Proto)
		diagnostics.SetUpstreamResponseIdentifiers(req.Context(), resp.Header)
		if resp.Body != nil {
			resp.Body = &upstreamDiagnosticBody{ReadCloser: resp.Body, ctx: req.Context()}
		}
	}

	if ginCtx != nil {
		storeUpstreamTiming(ginCtx, trace)
	}

	return resp, err
}

// IDs correlate a pooled connection within this process without disclosing
// remote/local addresses, auth identity or the raw connection pointer.
var upstreamConnectionIDSalt = func() [32]byte {
	var salt [32]byte
	_, _ = rand.Read(salt[:])
	return salt
}()

func upstreamConnectionID(conn net.Conn) string {
	hash := sha256.New()
	_, _ = hash.Write(upstreamConnectionIDSalt[:])
	_, _ = fmt.Fprintf(hash, "%p", conn)
	return hex.EncodeToString(hash.Sum(nil)[:12])
}

type upstreamDiagnosticBody struct {
	io.ReadCloser
	ctx context.Context
}

func (b *upstreamDiagnosticBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	diagnostics.RecordUpstreamBodyRead(b.ctx, n)
	if err != nil && err != io.EOF {
		diagnostics.SetUpstreamError(b.ctx, err)
	}
	return n, err
}

func storeUpstreamTiming(ginCtx *gin.Context, trace *upstreamTraceTimings) {
	if ginCtx == nil || trace == nil {
		return
	}
	ginCtx.Set(upstreamTimingKey, trace)
}

func upstreamTimingFromContext(ginCtx *gin.Context) map[string]any {
	if ginCtx == nil {
		return nil
	}
	value, exists := ginCtx.Get(upstreamTimingKey)
	if !exists {
		return nil
	}
	trace, ok := value.(*upstreamTraceTimings)
	if !ok || trace == nil {
		return nil
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()

	result := map[string]any{}
	if trace.Host != "" {
		result["host"] = trace.Host
	}
	if trace.Scheme != "" {
		result["scheme"] = trace.Scheme
	}
	result["got_conn_reused"] = trace.GotConnReused
	if trace.WasIdle {
		result["was_idle"] = true
		result["idle_ms"] = trace.IdleMs
	}
	if trace.ConnectMs > 0 {
		result["connect_ms"] = trace.ConnectMs
	}
	if trace.TLSMs > 0 {
		result["tls_ms"] = trace.TLSMs
	}
	if trace.HeaderTTFBMs > 0 {
		result["header_ttfb_ms"] = trace.HeaderTTFBMs
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
