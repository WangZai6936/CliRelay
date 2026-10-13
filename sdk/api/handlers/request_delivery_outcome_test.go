package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	requestusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type deliveryTestCapture struct{ records chan requestusage.Record }

func (p *deliveryTestCapture) HandleUsage(_ context.Context, r requestusage.Record) {
	if r.RequestOutcome != nil {
		select {
		case p.records <- r:
		default:
		}
	}
}

type failingDeliveryWriter struct{ http.ResponseWriter }

func (w failingDeliveryWriter) Write([]byte) (int, error) {
	return 0, errors.New("downstream write failed")
}

func TestRequestDeliveryOutcomes(t *testing.T) {
	capture := &deliveryTestCapture{records: make(chan requestusage.Record, 64)}
	requestusage.RegisterPlugin(capture)
	h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)
	for _, tc := range []struct {
		name, expected                                  string
		failAttempt, failWrite, cancelClient, failFinal bool
	}{
		{name: "normal_cleanup", expected: requestusage.OutcomeSucceeded},
		{name: "retry_then_delivered", failAttempt: true, expected: requestusage.OutcomeRecovered},
		{name: "ignored_write_error", failWrite: true, expected: requestusage.OutcomeClientDeliveryFailed},
		{name: "client_canceled", cancelClient: true, expected: requestusage.OutcomeClientCanceled},
		{name: "final_stream_error", failFinal: true, expected: requestusage.OutcomeFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, cancelBase := context.WithCancel(context.Background())
			defer cancelBase()
			ctx := requestusage.BeginRequest(base)
			var writer http.ResponseWriter = httptest.NewRecorder()
			if tc.failWrite {
				writer = failingDeliveryWriter{writer}
			}
			c, _ := gin.CreateTestContext(writer)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			child, finish := h.GetContextWithCancel(nil, c, ctx)
			if tc.failAttempt {
				requestusage.PublishRecord(child, requestusage.Record{APIKeyID: t.Name(), Failed: true})
			}
			requestusage.PublishRecord(child, requestusage.Record{APIKeyID: t.Name()})
			if tc.failWrite {
				_, _ = c.Writer.Write([]byte("response"))
			}
			if tc.cancelClient {
				cancelBase()
			}
			if tc.failFinal {
				finish(errors.New("upstream stream interrupted"))
			} else {
				finish()
			}
			requestusage.FinishRequest(ctx)
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			for {
				select {
				case record := <-capture.records:
					if record.APIKeyID != t.Name() {
						continue
					}
					if got := record.RequestOutcome.Result; got != tc.expected {
						t.Fatalf("outcome=%s want %s", got, tc.expected)
					}
					return
				case <-timer.C:
					t.Fatal("terminal request outcome missing")
				}
			}
		})
	}
}

func TestRequestDeliveryOutcomeKeepsAuthenticatedTenantBeforeExecutor(t *testing.T) {
	capture := &deliveryTestCapture{records: make(chan requestusage.Record, 16)}
	requestusage.RegisterPlugin(capture)
	h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)
	for _, tc := range []struct {
		name, expected string
		earlyCancel    bool
	}{
		{"unknown-model-before-executor", requestusage.OutcomeFailed, false},
		{"authenticated-early-client-cancel", requestusage.OutcomeClientCanceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, cancelBase := context.WithCancel(context.Background())
			defer cancelBase()
			outer := requestusage.BeginRequest(base)
			requestID := requestusage.RequestIdentity(outer)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(outer)
			const tenantID = "22222222-2222-2222-2222-222222222222"
			c.Set("tenantID", tenantID) // AuthMiddleware's trusted Gin identity.
			c.Set("apiKey", "authenticated-test-key")
			// Cover handlers which intentionally select a separate execution
			// parent: tracking and authenticated identity must still be inherited.
			child, finish := h.GetContextWithCancel(nil, c, context.Background())
			if tc.earlyCancel {
				cancelBase()
				finish(context.Canceled)
			} else {
				_, _, errMsg := h.ExecuteWithAuthManager(child, "openai", "unregistered-outcome-test-model", nil, "")
				if errMsg == nil || errMsg.Error == nil {
					t.Fatal("unknown model reached an executor instead of failing preflight")
				}
				finish(errMsg.Error)
			}
			requestusage.FinishRequest(outer)
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			for {
				select {
				case record := <-capture.records:
					if record.RequestCorrelationID != requestID {
						continue
					}
					if record.TrustedTenantID != tenantID || record.APIKey != "authenticated-test-key" {
						t.Fatalf("authenticated identity lost: tenant=%q key=%q", record.TrustedTenantID, record.APIKey)
					}
					if record.RequestOutcome == nil || record.RequestOutcome.Result != tc.expected || record.RequestOutcome.Attempts != 0 {
						t.Fatalf("pre-executor outcome = %+v", record.RequestOutcome)
					}
					if !tc.earlyCancel && record.Model != "unregistered-outcome-test-model" {
						t.Fatalf("model identity = %q", record.Model)
					}
					if record.InputContent != "" || record.OutputContent != "" || record.DetailContent != "" {
						t.Fatal("identity snapshot retained request/response body")
					}
					return
				case <-timer.C:
					t.Fatal("pre-executor terminal outcome missing")
				}
			}
		})
	}
}
