package auth

import (
	"context"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type bufferRecoveryKey struct{}

// A request-specific upstream buffer rejection permits one alternative call.
// This state survives handler bootstrap recovery and never damages auth health.
type bufferRecovery struct {
	mu        sync.Mutex
	started   bool
	remaining int
	blocked   map[string]struct{}
	deadline  time.Time
	lastErr   error
}

// WithRequestBufferRecovery shares the one-alternative limit across all layers.
func WithRequestBufferRecovery(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if bufferRecoveryFrom(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, bufferRecoveryKey{}, &bufferRecovery{blocked: make(map[string]struct{})})
}

func bufferRecoveryFrom(ctx context.Context) *bufferRecovery {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(bufferRecoveryKey{}).(*bufferRecovery)
	return state
}

func isUpstreamBufferRejection(err error) bool {
	return cliproxyexecutor.IsUpstreamRequestBufferLimit(err)
}

func (s *bufferRecovery) recordFailure(authID string, err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !cliproxyexecutor.IsUpstreamRequestBufferLimit(err) {
		if s.started {
			s.lastErr = err
		}
		return
	}
	if !s.started {
		s.started = true
		s.remaining = 1
		s.deadline = time.Now().Add(20 * time.Second)
	}
	if authID != "" {
		s.blocked[authID] = struct{}{}
	}
	s.lastErr = err
}

func (s *bufferRecovery) active() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *bufferRecovery) available(ctx context.Context) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.started || (s.remaining > 0 && time.Now().Before(s.deadline))
}

func (s *bufferRecovery) failure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil {
		return &Error{Code: "recovery_budget_exhausted", Message: "recovery unavailable", HTTPStatus: 502}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *bufferRecovery) waitingError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started && !time.Now().Before(s.deadline) && s.lastErr != nil {
		return s.lastErr
	}
	return err
}

func (s *bufferRecovery) consume(ctx context.Context, authID string) error {
	if s == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil
	}
	_, blocked := s.blocked[authID]
	if blocked || s.remaining == 0 || !time.Now().Before(s.deadline) {
		return s.lastErr
	}
	s.remaining--
	return nil
}

func (s *bufferRecovery) exclude(tried map[string]struct{}) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.blocked {
		tried[id] = struct{}{}
	}
}

func (s *bufferRecovery) waitingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s == nil {
		return ctx, func() {}
	}
	s.mu.Lock()
	deadline := s.deadline
	s.mu.Unlock()
	if deadline.IsZero() {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}
