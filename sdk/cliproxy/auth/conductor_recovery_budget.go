package auth

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type recoveryBudgetContextKey struct{}

// RequestRecoveryBudget bounds actual upstream calls across manager failover and
// handler bootstrap recovery. Its clock starts at the first failed attempt; it
// gates waits and new attempts without cancelling an established healthy stream.
type RequestRecoveryBudget struct {
	mu           sync.Mutex
	maxAttempts  int
	timeout      time.Duration
	attempts     int
	authAttempts map[string]int
	lastAuthID   string
	deadline     time.Time
	committed    bool
	lastErr      error
}

// WithRequestRecoveryBudget enables a shared budget. Non-positive limits retain
// the legacy behavior, and an existing request budget is never replaced.
func WithRequestRecoveryBudget(ctx context.Context, maxAttempts int, timeout time.Duration) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if RecoveryBudgetFromContext(ctx) != nil || maxAttempts <= 0 {
		return ctx
	}
	return context.WithValue(ctx, recoveryBudgetContextKey{}, &RequestRecoveryBudget{
		maxAttempts: maxAttempts, timeout: timeout, authAttempts: make(map[string]int),
	})
}

// RecoveryBudgetFromContext returns the request's shared recovery state, if enabled.
func RecoveryBudgetFromContext(ctx context.Context) *RequestRecoveryBudget {
	if ctx == nil {
		return nil
	}
	b, _ := ctx.Value(recoveryBudgetContextKey{}).(*RequestRecoveryBudget)
	return b
}

// ReserveAdditionalRecoveryAttempt accounts for a provider's internal replay
// (for example rejected encrypted reasoning or a stale WebSocket connection).
// The manager already reserved that executor's first call. Internal recovery
// must reserve before making another call and preserve its original error when
// the shared cap or recovery deadline disallows a replay.
func ReserveAdditionalRecoveryAttempt(ctx context.Context, previous error) error {
	b := RecoveryBudgetFromContext(ctx)
	if b == nil {
		return nil
	}
	b.recordFailure(previous)
	return b.consume(ctx, &Auth{ID: b.lastAuth()})
}

// MarkOutput permanently disables transparent replay for this request.
func (b *RequestRecoveryBudget) MarkOutput() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.committed = true
	b.mu.Unlock()
}

func (b *RequestRecoveryBudget) recordFailure(err error) {
	if b == nil || err == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastErr = err
	if b.deadline.IsZero() && b.timeout > 0 {
		b.deadline = time.Now().Add(b.timeout)
	}
}

func (b *RequestRecoveryBudget) available(ctx context.Context, wait time.Duration) bool {
	if b == nil {
		return true
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed || b.attempts >= b.maxAttempts {
		return false
	}
	now := time.Now()
	if !b.deadline.IsZero() && !now.Add(wait).Before(b.deadline) {
		return false
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !now.Add(wait).Before(deadline) {
			return false
		}
	}
	return true
}

func (b *RequestRecoveryBudget) consume(ctx context.Context, auth *Auth) error {
	if b == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed || b.attempts >= b.maxAttempts || (!b.deadline.IsZero() && !time.Now().Before(b.deadline)) {
		if b.lastErr != nil {
			return b.lastErr
		}
		return &Error{Code: "recovery_budget_exhausted", Message: "upstream recovery budget exhausted", HTTPStatus: 502}
	}
	b.attempts++
	if auth != nil {
		b.authAttempts[auth.ID]++
		b.lastAuthID = auth.ID
	}
	return nil
}

func (b *RequestRecoveryBudget) exhaustedError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if b != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.lastErr != nil {
			return b.lastErr
		}
	}
	return &Error{Code: "recovery_budget_exhausted", Message: "upstream recovery budget exhausted", HTTPStatus: 502}
}

func (b *RequestRecoveryBudget) usedAuths() map[string]struct{} {
	used := make(map[string]struct{})
	if b == nil {
		return used
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id := range b.authAttempts {
		used[id] = struct{}{}
	}
	return used
}

func (b *RequestRecoveryBudget) lastAuth() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastAuthID
}

func (b *RequestRecoveryBudget) attemptCount() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts
}

// waitingContext bounds only queueing, never the selected candidate's execution
// context. The latter continues to use the original client request context.
func (b *RequestRecoveryBudget) waitingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if b == nil {
		return ctx, func() {}
	}
	b.mu.Lock()
	deadline := b.deadline
	b.mu.Unlock()
	if deadline.IsZero() {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

func (b *RequestRecoveryBudget) waitingError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if b == nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.deadline.IsZero() && !time.Now().Before(b.deadline) && b.lastErr != nil {
		return b.lastErr
	}
	return err
}

func (b *RequestRecoveryBudget) authRetryAllowed(auth *Auth, defaultRetry int) bool {
	if b == nil || auth == nil {
		return true
	}
	b.mu.Lock()
	count := b.authAttempts[auth.ID]
	b.mu.Unlock()
	limit := defaultRetry
	if override, ok := auth.RequestRetryOverride(); ok {
		limit = override
	}
	return count <= max(0, limit)
}

// RecoverStream retries a pre-output stream failure using the same budget as
// initial HTTP failures. Selection still applies tenant/channel/group/model gates.
func (m *Manager) RecoverStream(ctx context.Context, providers []string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, previous error) (*cliproxyexecutor.StreamResult, error) {
	if isSinglePickRouteRequest(opts.Metadata) || isRequestInvalidError(previous) || isContentPolicyViolation(previous) {
		return nil, previous
	}
	b := RecoveryBudgetFromContext(ctx)
	if b == nil {
		return m.ExecuteStream(ctx, providers, req, opts)
	}
	b.recordFailure(previous)
	if isSinglePickRouteRequest(opts.Metadata) || isRequestInvalidError(previous) || isContentPolicyViolation(previous) || !b.available(ctx, 0) {
		return nil, previous
	}
	return m.ExecuteStream(ctx, providers, req, opts)
}

// recoveryWait only considers candidates allowed by the original request scope.
// It permits an idle alternative immediately; only a fully cooling pool waits.
func (m *Manager) recoveryWait(ctx context.Context, providers []string, model string, meta map[string]any, maxWait time.Duration) (time.Duration, bool) {
	b := RecoveryBudgetFromContext(ctx)
	if b == nil || isSinglePickRouteRequest(meta) || maxWait <= 0 {
		return 0, false
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		providerSet[provider] = struct{}{}
	}
	now := time.Now()
	scope := newSelectionScope(m.currentRuntimeConfigForTenant(tenantIDFromMetadata(meta)), model, meta)
	m.mu.RLock()
	defer m.mu.RUnlock()
	var found bool
	var wait time.Duration
	for _, candidate := range m.auths {
		if candidate == nil {
			continue
		}
		if _, ok := providerSet[candidate.Provider]; !ok || !b.authRetryAllowed(candidate, int(m.requestRetry.Load())) {
			continue
		}
		allowed := false
		for _, group := range scope.routeGroupsToTry() {
			if scope.allowsCandidate(candidate, group, nil, m.modelRegistry) {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		blocked, reason, next := isAuthBlockedForModel(candidate, model, now)
		candidateWait := time.Duration(0)
		if blocked {
			if reason == blockReasonDisabled || next.IsZero() {
				continue
			}
			candidateWait = max(0, next.Sub(now))
		}
		if state := m.overloadRecovery[overloadRecoveryKey(candidate.ID, model)]; state != nil {
			candidateWait = max(candidateWait, state.until.Sub(now))
			if state.probeInFlight {
				candidateWait = max(candidateWait, 100*time.Millisecond)
			}
		}
		if !found || candidateWait < wait {
			wait = candidateWait
			found = true
		}
	}
	if !found || wait > maxWait {
		return 0, false
	}
	// Add positive jitter only when waiting for a cooling pool, keeping healthy
	// alternate-account failover immediate and avoiding synchronized retries.
	if wait > 0 {
		wait += time.Duration(rand.Int64N(int64(250 * time.Millisecond)))
	}
	return wait, wait <= maxWait && b.available(ctx, wait)
}
