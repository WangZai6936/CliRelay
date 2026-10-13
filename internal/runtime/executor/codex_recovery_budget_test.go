package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexReasoningRepairSharesOuterRecoveryAttemptBudget(t *testing.T) {
	for _, limit := range []int{1, 3, 4} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			ctx := cliproxyauth.WithRequestRecoveryBudget(context.Background(), limit, 20*time.Second)
			body := []byte(`{"input":[{"type":"reasoning","encrypted_content":"legacy"}]}`)
			calls := 0
			client := &http.Client{Transport: xaiStreamRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				payload, _ := io.ReadAll(r.Body)
				responseBody := `{"error":{"code":"invalid_encrypted_content"}}`
				status := 400
				if !bytes.Contains(payload, []byte("encrypted_content")) {
					responseBody = "data: {\"type\":\"response.completed\"}\n\n"
					status = 200
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(responseBody)), Header: make(http.Header), Request: r}, nil
			})}
			for calls < limit {
				// Emulate the manager reserving one actual call before entering the
				// executor; the helper must reserve its own optional second call.
				if err := cliproxyauth.ReserveAdditionalRecoveryAttempt(ctx, errors.New("previous failure")); err != nil {
					t.Fatal(err)
				}
				req, _ := http.NewRequestWithContext(ctx, "POST", "https://test.invalid/responses", bytes.NewReader(body))
				resp, _, err := doCodexRequestWithReasoningRecovery(ctx, client, req, body, UpstreamRecorder{})
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			if calls != limit {
				t.Fatalf("actual HTTP calls=%d shared cap=%d", calls, limit)
			}
			if err := cliproxyauth.ReserveAdditionalRecoveryAttempt(ctx, errors.New("overload")); err == nil {
				t.Fatal("internal retry exceeded shared cap")
			}
		})
	}
}

func TestCodexWebsocketHTTPFallbackSharesBudgetAndReleasesSession(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Header.Get("Upgrade") == "websocket" {
					w.WriteHeader(http.StatusUpgradeRequired)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer server.Close()
			ctx := cliproxyauth.WithRequestRecoveryBudget(context.Background(), limit, 20*time.Second)
			if err := cliproxyauth.ReserveAdditionalRecoveryAttempt(ctx, errors.New("manager initial reservation")); err != nil {
				t.Fatal(err)
			}
			e := NewCodexWebsocketsExecutor(&config.Config{})
			a := &cliproxyauth.Auth{ID: "budget-ws", Provider: "codex", Attributes: map[string]string{"api_key": "mock-key", "base_url": server.URL}}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex"), Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "budget-session"}}
			stream, err := e.ExecuteStream(ctx, a, cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"model":"gpt-5-codex","input":[]}`)}, opts)
			if limit == 1 && err == nil {
				t.Fatal("WS426 fallback exceeded budget")
			}
			if limit == 2 && err != nil {
				t.Fatal(err)
			}
			if stream != nil {
				for chunk := range stream.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			}
			if calls != limit {
				t.Fatalf("actual WS/HTTP requests=%d cap=%d", calls, limit)
			}
			session := e.getOrCreateSession("budget-session")
			if !session.reqMu.TryLock() {
				t.Fatal("fallback leaked session request lock")
			}
			session.reqMu.Unlock()
		})
	}
}

func TestCodexHTTP503CarriesProviderRetryAfterIntoRecovery(t *testing.T) {
	for _, header := range []http.Header{{"Retry-After": []string{"30"}}, {"Retry-After": []string{time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)}}} {
		err := newCodexStatusErr(503, []byte(`{"error":{"message":"unavailable"}}`), header)
		if delay := err.RetryAfter(); delay == nil || *delay < 29*time.Second || *delay > 31*time.Second {
			t.Fatalf("503 Retry-After was lost: %v", delay)
		}
	}
}
