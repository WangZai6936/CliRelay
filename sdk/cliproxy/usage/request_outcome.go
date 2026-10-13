package usage

import (
	"context"
	"sync"
	"time"
)

const (
	OutcomeSucceeded            = "succeeded"
	OutcomeRecovered            = "recovered"
	OutcomeFailed               = "failed"
	OutcomeClientCanceled       = "client_canceled"
	OutcomeClientDeliveryFailed = "client_delivery_failed"
	OutcomeUnknown              = "unknown"
	FailureBeforeOutput         = "before_output"
	FailureAfterOutput          = "after_output"
)

// RequestOutcome is emitted by the incoming request lifecycle, independently
// of provider billing records. Missing lifecycle evidence remains unknown.
type RequestOutcome struct {
	RequestID      string    `json:"request_id"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	Result         string    `json:"result"`
	FailureStage   string    `json:"failure_stage,omitempty"`
	Attempts       int64     `json:"attempts"`
	FailedAttempts int64     `json:"failed_attempts"`
}

type requestTrackerKey struct{}
type requestTracker struct {
	mu             sync.Mutex
	id             string
	started        time.Time
	result         string
	output         bool
	finished       bool
	attempts       int64
	failedAttempts int64
	identity       Record
}

// BeginRequest gives one logical HTTP request (or one WebSocket generation)
// its own tracker. A fresh random identity prevents compact log-ID collisions.
func BeginRequest(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestTrackerKey{}, &requestTracker{id: NewIdempotencyKey(), started: time.Now()})
}

// WithRequestTracker carries request tracking when a handler intentionally
// chooses a different execution parent. An existing logical-turn tracker wins.
func WithRequestTracker(ctx, source context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if trackerFromContext(ctx) != nil {
		return ctx
	}
	if t := trackerFromContext(source); t != nil {
		return context.WithValue(ctx, requestTrackerKey{}, t)
	}
	return ctx
}

func RequestIdentity(ctx context.Context) string {
	if t := trackerFromContext(ctx); t != nil {
		return t.id
	}
	return ""
}

// ObserveRequestIdentity supplies metadata for canceled operations that never
// emitted a billing record. It does not manufacture a provider attempt.
func ObserveRequestIdentity(ctx context.Context, record Record) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.finished {
			return
		}
		identity := requestIdentityRecord(record)
		for _, field := range []struct {
			target *string
			value  string
		}{
			{&t.identity.TrustedTenantID, identity.TrustedTenantID}, {&t.identity.APIKey, identity.APIKey},
			{&t.identity.APIKeyID, identity.APIKeyID}, {&t.identity.APIKeyName, identity.APIKeyName},
			{&t.identity.Model, identity.Model}, {&t.identity.AuthIndex, identity.AuthIndex},
			{&t.identity.AuthSubjectID, identity.AuthSubjectID}, {&t.identity.ChannelName, identity.ChannelName},
			{&t.identity.APIIdentifier, identity.APIIdentifier}, {&t.identity.Source, identity.Source}, {&t.identity.RequestID, identity.RequestID},
		} {
			if *field.target == "" {
				*field.target = field.value
			}
		}
	}
}

func requestIdentityRecord(record Record) Record {
	return Record{TrustedTenantID: record.TrustedTenantID, APIKey: record.APIKey,
		APIKeyID: record.APIKeyID, APIKeyName: record.APIKeyName, Model: record.Model,
		AuthIndex: record.AuthIndex, AuthSubjectID: record.AuthSubjectID, ChannelName: record.ChannelName,
		APIIdentifier: record.APIIdentifier, Source: record.Source, RequestID: record.RequestID}
}

func trackerFromContext(ctx context.Context) *requestTracker {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(requestTrackerKey{}).(*requestTracker)
	return t
}

// MarkRequestOutput records delivery of meaningful model output, rather than
// keepalives, role headers, or internal provider startup frames.
func MarkRequestOutput(ctx context.Context) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		t.output = true
		t.mu.Unlock()
	}
}

// MarkRequestSucceeded is called after a normal final response was delivered.
func MarkRequestSucceeded(ctx context.Context) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		if t.result == "" {
			t.result = OutcomeSucceeded
		}
		t.mu.Unlock()
	}
}

// MarkRequestFailed records the final client-facing error after retries end.
func MarkRequestFailed(ctx context.Context) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		if t.result != OutcomeClientCanceled && t.result != OutcomeClientDeliveryFailed {
			t.result = OutcomeFailed
		}
		t.mu.Unlock()
	}
}

// MarkRequestCanceled is explicit client cancellation, independent of the
// provider's own context or a proxy-created cleanup cancellation.
func MarkRequestCanceled(ctx context.Context) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		if t.result != OutcomeClientDeliveryFailed && t.result != OutcomeSucceeded {
			t.result = OutcomeClientCanceled
		}
		t.mu.Unlock()
	}
}

// MarkRequestDeliveryFailed records a downstream write failure separately from
// an upstream error; later cleanup/success hooks cannot overwrite it.
func MarkRequestDeliveryFailed(ctx context.Context) {
	if t := trackerFromContext(ctx); t != nil {
		t.mu.Lock()
		t.result = OutcomeClientDeliveryFailed
		t.mu.Unlock()
	}
}

func attachRequestAttempt(ctx context.Context, record *Record) {
	t := trackerFromContext(ctx)
	if t == nil || record == nil || record.RequestOutcome != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// A publisher can finish immediately after a canceled handler returns. Keep
	// its attempt correlated even though the terminal outcome is already fixed.
	record.RequestCorrelationID = t.id
	n := record.ProviderAttempts
	if n < 1 {
		n = 1
	}
	t.attempts += n
	if record.Failed || record.ResponseStatus >= 400 {
		t.failedAttempts += n
	} else if n > 1 {
		t.failedAttempts += n - 1
	}
	record.Attempt = t.attempts
	if record.FailureStage == "" && record.Failed {
		record.FailureStage = FailureBeforeOutput
		if t.output {
			record.FailureStage = FailureAfterOutput
		}
	}
	// Only identity fields are retained; never hold prompts, responses or files.
	t.identity = requestIdentityRecord(*record)
}

// FinishRequest emits exactly one terminal record when its handler returns.
// Provider successes alone never imply successful delivery to the client.
func FinishRequest(ctx context.Context) {
	t := trackerFromContext(ctx)
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.finished || (t.attempts == 0 && t.result == "") {
		t.mu.Unlock()
		return
	}
	t.finished = true
	result := t.result
	if result != OutcomeSucceeded && result != OutcomeClientDeliveryFailed && ctx.Err() != nil {
		result = OutcomeClientCanceled
	}
	if result == "" {
		result = OutcomeUnknown
	}
	if result == OutcomeSucceeded && t.failedAttempts > 0 {
		result = OutcomeRecovered
	}
	stage := ""
	if result == OutcomeFailed || result == OutcomeClientCanceled || result == OutcomeClientDeliveryFailed {
		stage = FailureBeforeOutput
		if t.output {
			stage = FailureAfterOutput
		}
	}
	record := t.identity
	record.RequestedAt = t.started
	record.RequestCorrelationID = t.id
	record.RequestOutcome = &RequestOutcome{RequestID: t.id, StartedAt: t.started, CompletedAt: time.Now(),
		Result: result, FailureStage: stage, Attempts: t.attempts, FailedAttempts: t.failedAttempts}
	t.mu.Unlock()
	PublishRecord(context.Background(), record)
}
