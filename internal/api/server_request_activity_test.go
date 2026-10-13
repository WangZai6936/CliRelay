package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func activityTestServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	t.Cleanup(s.mgmt.Close)
	s.engine.GET("/v0/management/activity-test", s.mgmt.GetSystemStats)
	return s
}

func activeRequestsForTest(t *testing.T, s *Server) int64 {
	t.Helper()
	w := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v0/management/activity-test", nil))
	var stats map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	raw, ok := stats["active_api_requests"]
	if !ok || string(raw) == "null" {
		t.Fatal("active request count is unavailable")
	}
	var count int64
	if err := json.Unmarshal(raw, &count); err != nil {
		t.Fatalf("decode active count: %v", err)
	}
	return count
}

func waitActivitySignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the expected state")
	}
}

func TestActiveAPIRequestsTracksStreamsWithoutQuotaAndIsNodeLocal(t *testing.T) {
	s := activityTestServer(t)
	other := activityTestServer(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s.engine.POST("/activity-stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Writer.WriteHeader(http.StatusOK)
		c.Writer.Flush()
		close(entered)
		<-release
	})
	go func() {
		defer close(done)
		s.server.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/activity-stream", nil))
	}()
	waitActivitySignal(t, entered)
	if got := activeRequestsForTest(t, s); got != 1 {
		t.Fatalf("active stream count = %d, want 1", got)
	}
	if got := activeRequestsForTest(t, other); got != 0 {
		t.Fatalf("another server's count = %d, want 0", got)
	}
	unblock()
	waitActivitySignal(t, done)
	if got := activeRequestsForTest(t, s); got != 0 {
		t.Fatalf("count after completed stream = %d, want 0", got)
	}
}

type activityGatedBody struct {
	io.Reader
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *activityGatedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.Reader.Read(p)
}

func (b *activityGatedBody) Close() error { return nil }

func TestActiveAPIRequestsIncludesBodyProcessingBeforeGinAdmission(t *testing.T) {
	s := activityTestServer(t)
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	_, _ = w.Write([]byte(`{"model":"test"}`))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s.engine.POST("/activity-body", func(c *gin.Context) {
		_, _ = io.Copy(io.Discard, c.Request.Body)
		c.Status(http.StatusNoContent)
	})
	body := &activityGatedBody{Reader: bytes.NewReader(compressed.Bytes()), entered: entered, release: release}
	req := httptest.NewRequest(http.MethodPost, "/activity-body", body)
	req.Header.Set("Content-Encoding", "gzip")
	go func() {
		defer close(done)
		s.server.Handler.ServeHTTP(httptest.NewRecorder(), req)
	}()
	waitActivitySignal(t, entered)
	if got := activeRequestsForTest(t, s); got != 1 {
		t.Fatalf("count during decompression = %d, want 1", got)
	}
	unblock()
	waitActivitySignal(t, done)
	if got := activeRequestsForTest(t, s); got != 0 {
		t.Fatalf("count after body processing = %d, want 0", got)
	}
}

func TestActiveAPIRequestsReleasesOnCancellationAndPanic(t *testing.T) {
	s := activityTestServer(t)
	entered, done := make(chan struct{}), make(chan struct{})
	s.engine.POST("/activity-cancel", func(c *gin.Context) {
		close(entered)
		<-c.Request.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodPost, "/activity-cancel", nil).WithContext(ctx)
		s.server.Handler.ServeHTTP(httptest.NewRecorder(), req)
	}()
	waitActivitySignal(t, entered)
	if got := activeRequestsForTest(t, s); got != 1 {
		t.Fatalf("count before cancellation = %d, want 1", got)
	}
	cancel()
	waitActivitySignal(t, done)
	s.engine.POST("/activity-panic", func(*gin.Context) { panic("activity test") })
	w := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/activity-panic", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("panic response = %d, want 500", w.Code)
	}
	if got := activeRequestsForTest(t, s); got != 0 {
		t.Fatalf("count after cancellation and panic = %d, want 0", got)
	}
}

func TestActiveAPIRequestsIncludesOpenResponsesWebSocketHandler(t *testing.T) {
	s := activityTestServer(t)
	done := make(chan struct{})
	s.engine.GET("/activity-websocket", func(c *gin.Context) {
		conn, err := (&websocket.Upgrader{}).Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
	})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		s.server.Handler.ServeHTTP(w, r)
	}))
	defer httpServer.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/activity-websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := activeRequestsForTest(t, s); got != 1 {
		t.Fatalf("count with idle WebSocket handler = %d, want 1", got)
	}
	_ = conn.Close()
	waitActivitySignal(t, done)
	if got := activeRequestsForTest(t, s); got != 0 {
		t.Fatalf("count after WebSocket close = %d, want 0", got)
	}
}
