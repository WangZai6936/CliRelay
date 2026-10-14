package usage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
)

// RequestFailure contains compact diagnostics, never provider messages or bodies.
type RequestFailure struct {
	ResponseStatus int    `json:"response_status,omitempty"`
	ErrorStatus    int    `json:"error_status,omitempty"`
	ErrorCode      string `json:"error_code,omitempty"`
	ErrorType      string `json:"error_type,omitempty"`
	ErrorReason    string `json:"error_reason,omitempty"`
	ErrorPhase     string `json:"error_phase,omitempty"`
}

var diagnosticCode = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,79}$`)

func safeDiagnosticCode(value string) string {
	value = strings.TrimSpace(value)
	if diagnosticCode.MatchString(value) && !strings.HasPrefix(value, "sk-") && !strings.HasPrefix(value, "sk_") {
		return value
	}
	return ""
}

// ObserveRequestPhase identifies where a zero-attempt failure was waiting.
func ObserveRequestPhase(ctx context.Context, phase string) {
	switch phase {
	case "admission", "model_resolution", "account_selection", "account_queue", "cooldown_wait", "upstream":
	default:
		return
	}
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.finished {
			t.failure.ErrorPhase = phase
		}
	}
}

// ObserveRequestHTTPStatus is the status actually committed on the client wire.
// A stream can have HTTP 200 while its terminal error has a different status.
func ObserveRequestHTTPStatus(ctx context.Context, status int) {
	if status < 100 || status > 599 {
		return
	}
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.finished {
			t.failure.ResponseStatus = status
		}
	}
}

// ObserveRequestError keeps stable machine codes and classified reasons only.
func ObserveRequestError(ctx context.Context, status int, err error) {
	t := trackerFromContext(ctx)
	if t == nil {
		return
	}
	detail := classifyRequestError(status, err)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	// Generic JSON serialization must not erase an already observed typed cause.
	if t.failure.ErrorCode == "" || t.failure.ErrorCode == "unknown_error" || detail.ErrorCode == t.failure.ErrorCode || detail.ErrorCode == "context_canceled" || detail.ErrorCode == "deadline_exceeded" {
		t.failure.ErrorStatus = detail.ErrorStatus
		t.failure.ErrorCode = detail.ErrorCode
		t.failure.ErrorType = detail.ErrorType
		t.failure.ErrorReason = detail.ErrorReason
	}
	if t.failure.ErrorStatus == 0 && detail.ErrorStatus > 0 {
		t.failure.ErrorStatus = detail.ErrorStatus
	}
}

// MarkRequestFailedWithError captures the final cause before handler cleanup.
func MarkRequestFailedWithError(ctx context.Context, err error) {
	ObserveRequestError(ctx, 0, err)
	MarkRequestFailed(ctx)
}

func classifyRequestError(status int, err error) RequestFailure {
	d := RequestFailure{ErrorStatus: status, ErrorCode: "unknown_error", ErrorReason: "unclassified_failure"}
	if err != nil {
		var sc interface{ StatusCode() int }
		if errors.As(err, &sc) && sc.StatusCode() > 0 {
			d.ErrorStatus = sc.StatusCode()
		}
		var ec interface{ ErrorCode() string }
		if errors.As(err, &ec) {
			if code := safeDiagnosticCode(ec.ErrorCode()); code != "" {
				d.ErrorCode = code
			}
		}
		// Parse only structured codes. Raw messages are used for finite predicates,
		// and are never copied into the tracker, spool, database or management API.
		if text := err.Error(); len(text) <= 64*1024 {
			var payload struct {
				Code  string `json:"code"`
				Type  string `json:"type"`
				Error struct {
					Code string `json:"code"`
					Type string `json:"type"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(text), &payload) == nil {
				for _, code := range []string{payload.Error.Code, payload.Code} {
					if safe := safeDiagnosticCode(code); safe != "" {
						d.ErrorCode = safe
						break
					}
				}
				for _, kind := range []string{payload.Error.Type, payload.Type} {
					if safe := safeDiagnosticCode(kind); safe != "" {
						d.ErrorType = safe
						break
					}
				}
			}
		}
		switch {
		case errors.Is(err, context.Canceled):
			d.ErrorCode = "context_canceled"
			d.ErrorReason = "request_context_canceled"
		case errors.Is(err, context.DeadlineExceeded):
			d.ErrorCode = "deadline_exceeded"
			d.ErrorReason = "request_deadline_exceeded"
		case strings.Contains(strings.ToLower(err.Error()), "exceeded request buffer limit while retrying upstream"):
			d.ErrorCode = "upstream_request_buffer_limit"
			d.ErrorReason = "upstream_retry_buffer_exhausted"
		case strings.Contains(err.Error(), "stream error") && strings.Contains(err.Error(), "INTERNAL_ERROR"):
			d.ErrorCode = "upstream_http2_reset"
			d.ErrorReason = "peer_reset_during_http2_stream"
		}
	}
	if d.ErrorStatus < 100 || d.ErrorStatus > 599 {
		d.ErrorStatus = 0
	}
	switch d.ErrorCode {
	case "auth_not_found", "auth_unavailable", "provider_not_found", "no_available_auth":
		d.ErrorReason = "no_eligible_account"
	case "auth_cooldown", "peer_cooldown", "model_cooldown":
		d.ErrorReason = "accounts_cooling_down"
	case "auth_retry_exhausted", "recovery_budget_exhausted":
		d.ErrorReason = "recovery_budget_exhausted"
	case "model_not_found", "unsupported_model", "model_disabled":
		d.ErrorReason = "model_unavailable"
	case "server_is_overloaded":
		d.ErrorReason = "upstream_overloaded"
	case "rate_limit_exceeded":
		d.ErrorReason = "request_rate_limited"
	}
	if d.ErrorCode == "unknown_error" {
		switch d.ErrorStatus {
		case http.StatusTooManyRequests:
			d.ErrorCode = "rate_limit_exceeded"
			d.ErrorReason = "request_rate_limited"
		case http.StatusUnauthorized:
			d.ErrorCode = "authentication_failed"
			d.ErrorReason = "authentication_rejected"
		case http.StatusForbidden:
			d.ErrorCode = "permission_denied"
			d.ErrorReason = "request_not_permitted"
		case http.StatusBadRequest:
			d.ErrorCode = "invalid_request"
			d.ErrorReason = "request_validation_failed"
		case http.StatusRequestEntityTooLarge:
			d.ErrorCode = "request_body_too_large"
			d.ErrorReason = "request_body_limit_exceeded"
		}
	}
	if d.ErrorType == "" {
		switch d.ErrorCode {
		case "context_canceled", "deadline_exceeded":
			d.ErrorType = "context_error"
		case "upstream_http2_reset":
			d.ErrorType = "transport_error"
		case "upstream_request_buffer_limit":
			d.ErrorType = "upstream_error"
		case "auth_not_found", "auth_unavailable", "provider_not_found", "no_available_auth", "auth_cooldown", "peer_cooldown", "model_cooldown", "auth_retry_exhausted", "recovery_budget_exhausted":
			d.ErrorType = "selection_error"
		default:
			if d.ErrorStatus >= 500 {
				d.ErrorType = "server_error"
			}
		}
	}
	return d
}
