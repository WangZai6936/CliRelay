package usage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type terminalCause struct {
	code   string
	status int
}

func (e terminalCause) Error() string     { return "private upstream message" }
func (e terminalCause) ErrorCode() string { return e.code }
func (e terminalCause) StatusCode() int   { return e.status }

func TestTerminalDiagnosticsPreserveTypedCauseAndSeparateWireStatus(t *testing.T) {
	ctx := BeginRequest(context.Background())
	ObserveRequestPhase(ctx, "account_selection")
	ObserveRequestError(ctx, 0, terminalCause{"auth_not_found", 503})
	ObserveRequestError(ctx, 400, errors.New(`{"error":{"message":"private request sk-secret"}}`))
	ObserveRequestHTTPStatus(ctx, 200)
	d := trackerFromContext(ctx).failure
	if d.ResponseStatus != 200 || d.ErrorStatus != 503 || d.ErrorCode != "auth_not_found" || d.ErrorReason != "no_eligible_account" || d.ErrorPhase != "account_selection" {
		t.Fatalf("detail=%+v", d)
	}
	data, _ := json.Marshal(d)
	if strings.Contains(string(data), "private") || strings.Contains(string(data), "sk-secret") {
		t.Fatalf("raw content retained: %s", data)
	}
}
func TestTerminalErrorClassifier(t *testing.T) {
	for _, tc := range []struct {
		err          error
		status       int
		code, reason string
	}{
		{errors.New(`{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"sensitive"}}`), 429, "rate_limit_exceeded", "request_rate_limited"},
		{errors.New("exceeded request buffer limit while retrying upstream"), 507, "upstream_request_buffer_limit", "upstream_retry_buffer_exhausted"},
		{errors.New("stream error: stream ID 5; INTERNAL_ERROR; received from peer"), 502, "upstream_http2_reset", "peer_reset_during_http2_stream"},
		{context.Canceled, 499, "context_canceled", "request_context_canceled"},
		{context.DeadlineExceeded, 499, "deadline_exceeded", "request_deadline_exceeded"},
		{errors.New("private unexplained failure"), 503, "unknown_error", "unclassified_failure"},
	} {
		d := classifyRequestError(tc.status, tc.err)
		if d.ErrorCode != tc.code || d.ErrorReason != tc.reason {
			t.Fatalf("detail=%+v want=%s/%s", d, tc.code, tc.reason)
		}
	}
}
func TestTerminalDiagnosticsZeroAttemptsRecoveryAndCancellation(t *testing.T) {
	previous := defaultManager
	defaultManager = NewManager(16)
	p := &observingPlugin{seen: make(chan Record, 16)}
	defaultManager.Register(p)
	t.Cleanup(func() { defaultManager.Stop(); defaultManager.Wait(context.Background()); defaultManager = previous })
	for _, mode := range []string{"zero", "recovered", "canceled"} {
		base, cancel := context.WithCancel(context.Background())
		ctx := BeginRequest(base)
		ObserveRequestPhase(ctx, "account_queue")
		ObserveRequestHTTPStatus(ctx, 200)
		ObserveRequestError(ctx, 0, terminalCause{"auth_not_found", 503})
		if mode != "recovered" {
			MarkRequestFailed(ctx)
		}
		if mode == "recovered" {
			PublishRecord(ctx, Record{Failed: true})
			PublishRecord(ctx, Record{})
			MarkRequestSucceeded(ctx)
		}
		if mode == "canceled" {
			cancel()
		}
		FinishRequest(ctx)
		cancel()
		for {
			select {
			case r := <-p.seen:
				if r.RequestOutcome == nil {
					continue
				}
				o := r.RequestOutcome
				if mode == "zero" && (o.Attempts != 0 || o.ErrorCode != "auth_not_found" || o.Result != OutcomeFailed) {
					t.Fatalf("zero=%+v", o)
				}
				if mode == "recovered" && (o.Result != OutcomeRecovered || o.ErrorCode != "" || o.ErrorPhase != "" || o.ResponseStatus != 200) {
					t.Fatalf("recovered=%+v", o)
				}
				if mode == "canceled" && (o.Result != OutcomeClientCanceled || o.ErrorCode != "context_canceled" || o.ErrorStatus != 499) {
					t.Fatalf("canceled=%+v", o)
				}
				goto next
			case <-time.After(time.Second):
				t.Fatal("missing terminal record")
			}
		}
	next:
	}
}
