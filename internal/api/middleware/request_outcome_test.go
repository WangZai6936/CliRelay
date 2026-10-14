package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	requestusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type outcomeObserver struct {
	active atomic.Bool
	seen   chan requestusage.Record
}

func (p *outcomeObserver) HandleUsage(_ context.Context, r requestusage.Record) {
	if p.active.Load() && r.RequestOutcome != nil {
		select {
		case p.seen <- r:
		default:
		}
	}
}

func TestRequestOutcomeMiddlewareCapturesAdmissionAndStreamErrors(t *testing.T) {
	p := &outcomeObserver{seen: make(chan requestusage.Record, 16)}
	p.active.Store(true)
	requestusage.RegisterPlugin(p)
	t.Cleanup(func() { p.active.Store(false) })
	for _, tc := range []struct {
		status       int
		stream       bool
		code, reason string
	}{
		{429, false, "rate_limit_exceeded", "request_rate_limited"},
		{403, false, "permission_denied", "request_not_permitted"},
		{503, false, "unknown_error", "unclassified_failure"},
		{200, true, "upstream_request_buffer_limit", "upstream_retry_buffer_exhausted"},
	} {
		router := gin.New()
		router.Use(RequestOutcomeMiddleware())
		router.POST("/request", func(c *gin.Context) {
			if tc.stream {
				requestusage.ObserveRequestPhase(c.Request.Context(), "upstream")
				requestusage.ObserveRequestError(c.Request.Context(), 507, errors.New("exceeded request buffer limit while retrying upstream"))
				requestusage.MarkRequestFailed(c.Request.Context())
				c.String(200, "data: error\n\n")
				return
			}
			c.AbortWithStatusJSON(tc.status, gin.H{"error": gin.H{"message": "private prompt must not persist"}})
		})
		ctx := requestusage.BeginRequest(context.Background())
		id := requestusage.RequestIdentity(ctx)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/request", nil).WithContext(ctx)
		router.ServeHTTP(w, req)
		requestusage.FinishRequest(ctx)
		if w.Code != tc.status {
			t.Fatalf("status=%d want=%d", w.Code, tc.status)
		}
		for {
			select {
			case r := <-p.seen:
				if r.RequestOutcome.RequestID != id {
					continue
				}
				o := r.RequestOutcome
				if o.Attempts != 0 || o.Result != requestusage.OutcomeFailed || o.ResponseStatus != tc.status || o.ErrorCode != tc.code || o.ErrorReason != tc.reason {
					t.Fatalf("outcome=%+v", o)
				}
				if tc.stream && o.ErrorStatus != 507 {
					t.Fatalf("stream lost error status: %+v", o)
				}
				goto next
			case <-time.After(time.Second):
				t.Fatal("missing zero-attempt outcome")
			}
		}
	next:
	}
}
