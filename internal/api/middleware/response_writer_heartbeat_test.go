package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

func heartbeatTimingWriter(t *testing.T) (*gin.Context, *ResponseWriterWrapper, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := NewResponseWriterWrapper(c.Writer, nil, &RequestInfo{}, c)
	c.Writer = w
	return c, w, recorder
}

func firstApplicationWriteAt(t *testing.T, c *gin.Context) time.Time {
	t.Helper()
	raw, exists := c.Get(util.GinKeyFirstResponseAt)
	value, ok := raw.(time.Time)
	if !exists || !ok || value.IsZero() {
		t.Fatalf("missing application-write timestamp: %v", raw)
	}
	return value
}

func TestResponseWriterTransportHeartbeatDefersApplicationTiming(t *testing.T) {
	for _, tc := range []struct {
		name      string
		heartbeat string
		write     func(*ResponseWriterWrapper, string) (int, error)
	}{
		{"bytes", ": keep-alive\n\n", func(w *ResponseWriterWrapper, s string) (int, error) { return w.Write([]byte(s)) }},
		{"string", ": keep-alive\n\n", (*ResponseWriterWrapper).WriteString},
		{"custom", "event: ping\ndata: {\"transport\":true}\n\n", (*ResponseWriterWrapper).WriteString},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, w, recorder := heartbeatTimingWriter(t)
			handlers.WriteStreamKeepAlive(c, func() {
				if _, err := tc.write(w, tc.heartbeat); err != nil {
					t.Fatal(err)
				}
				w.Flush()
			})
			wireAt := w.firstChunkTimestamp
			if wireAt.IsZero() {
				t.Fatal("heartbeat did not record network first-byte time")
			}
			if _, exists := c.Get(util.GinKeyFirstResponseAt); exists {
				t.Fatal("heartbeat recorded application-write time")
			}
			if c.GetBool(requestctx.GinKeyTransportHeartbeat) {
				t.Fatal("heartbeat marker remained set")
			}
			payloadStarted := time.Now()
			if _, err := tc.write(w, "data: {\"content\":\"ready\"}\n\n"); err != nil {
				t.Fatal(err)
			}
			applicationAt := firstApplicationWriteAt(t, c)
			if applicationAt.Before(payloadStarted) || applicationAt.Before(wireAt) {
				t.Fatal("application timing preceded its payload")
			}
			if w.firstChunkTimestamp != wireAt {
				t.Fatal("application payload replaced network first-byte time")
			}
			if _, err := tc.write(w, "data: second\n\n"); err != nil {
				t.Fatal(err)
			}
			if got := firstApplicationWriteAt(t, c); got != applicationAt {
				t.Fatal("later payload replaced application timing")
			}
			if !strings.Contains(recorder.Body.String(), tc.heartbeat) || !strings.Contains(recorder.Body.String(), "ready") {
				t.Fatal("timing classification changed response bytes")
			}
		})
	}
}

func TestResponseWriterUnmarkedWritesKeepExistingTiming(t *testing.T) {
	for _, write := range []func(*ResponseWriterWrapper, string) (int, error){
		func(w *ResponseWriterWrapper, s string) (int, error) { return w.Write([]byte(s)) },
		(*ResponseWriterWrapper).WriteString,
	} {
		c, w, _ := heartbeatTimingWriter(t)
		if _, err := write(w, ": keep-alive\n\n"); err != nil {
			t.Fatal(err)
		}
		if got := firstApplicationWriteAt(t, c); got != w.firstChunkTimestamp {
			t.Fatal("unmarked first write changed timing behavior")
		}
	}
}

func TestResponseWriterPreservesExistingApplicationTiming(t *testing.T) {
	c, w, _ := heartbeatTimingWriter(t)
	existing := time.Now().Add(-time.Second)
	c.Set(util.GinKeyFirstResponseAt, existing)
	if _, err := w.Write([]byte("data: payload\n\n")); err != nil {
		t.Fatal(err)
	}
	if got := firstApplicationWriteAt(t, c); got != existing {
		t.Fatal("existing application timing was overwritten")
	}
	if w.firstChunkTimestamp.IsZero() {
		t.Fatal("existing application timing prevented network timing")
	}
}

func TestForwardStreamCustomHeartbeatDefersApplicationTiming(t *testing.T) {
	c, w, recorder := heartbeatTimingWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Request = c.Request.WithContext(ctx)
	data := make(chan []byte, 1)
	interval := time.Millisecond
	var wireAt, applicationStarted time.Time
	heartbeatWritten := false
	forwardCanceled := false
	h := &handlers.BaseAPIHandler{}
	h.ForwardStream(c, w, func(error) { forwardCanceled = true }, data, nil, handlers.StreamForwardOptions{
		KeepAliveInterval: &interval,
		WriteKeepAlive: func() {
			if heartbeatWritten {
				return
			}
			heartbeatWritten = true
			if _, err := w.WriteString("event: ping\ndata: transport\n\n"); err != nil {
				t.Fatal(err)
			}
			wireAt = w.firstChunkTimestamp
			if _, exists := c.Get(util.GinKeyFirstResponseAt); exists {
				t.Fatal("custom forwarder heartbeat recorded application timing")
			}
			data <- []byte("data: application\n\n")
			close(data)
		},
		WriteChunk: func(chunk []byte) {
			applicationStarted = time.Now()
			if _, err := w.Write(chunk); err != nil {
				t.Fatal(err)
			}
		},
	})
	if !heartbeatWritten || !forwardCanceled || applicationStarted.IsZero() {
		t.Fatal("forwarder did not finish the heartbeat/payload sequence")
	}
	if wireAt.IsZero() || w.firstChunkTimestamp != wireAt || firstApplicationWriteAt(t, c).Before(applicationStarted) {
		t.Fatal("forwarder did not preserve separate network/application timings")
	}
	if c.GetBool(requestctx.GinKeyTransportHeartbeat) || !strings.Contains(recorder.Body.String(), "data: application") {
		t.Fatal("custom heartbeat marker affected the application payload")
	}
}
