package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

func TestRequestContextOrBackgroundUsesRequestContextWhenPresent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	reqCtx, cancel := context.WithCancel(req.Context())
	c.Request = req.WithContext(reqCtx)

	ctx := requestContextOrBackground(c)
	cancel()

	select {
	case <-ctx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected requestContextOrBackground to return the request context when present")
	}
}

func TestGetContextWithCancelPreservesParentAndBridgesRequestCancellation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	type parentValueKey struct{}
	for _, mode := range []string{"request", "parent", "handler", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			requestContext, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			deadline := time.Now().Add(time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(25 * time.Millisecond)
			}
			parent, cancelParent := context.WithDeadline(context.WithValue(context.Background(), parentValueKey{}, "caller value"), deadline)
			defer cancelParent()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(requestContext)
			h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)
			child, finish := h.GetContextWithCancel(nil, c, parent)
			defer finish()
			if child.Value(parentValueKey{}) != "caller value" || child.Value(requestctx.ContextKeyGin) != c {
				t.Fatal("caller values or Gin context lost")
			}
			if got, ok := child.Deadline(); !ok || !got.Equal(deadline) {
				t.Fatalf("deadline=%v present=%v want=%v", got, ok, deadline)
			}
			switch mode {
			case "request":
				cancelRequest()
			case "parent":
				cancelParent()
			case "handler":
				finish()
			}
			select {
			case <-child.Done():
			case <-time.After(time.Second):
				t.Fatal("child did not observe cancellation/deadline")
			}
			if mode == "handler" && (parent.Err() != nil || requestContext.Err() != nil) {
				t.Fatal("handler cleanup canceled its parent or original HTTP request")
			}
			if mode == "deadline" && child.Err() != context.DeadlineExceeded {
				t.Fatalf("deadline error=%v", child.Err())
			}
		})
	}
}

func TestRequestContextOrBackgroundFallsBackWhenRequestMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)

	ctx := requestContextOrBackground(c)
	if ctx == nil {
		t.Fatal("expected non-nil fallback context")
	}

	select {
	case <-ctx.Done():
		t.Fatal("fallback context should not be canceled")
	default:
	}
}
