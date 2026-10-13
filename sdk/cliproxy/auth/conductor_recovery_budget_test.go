package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestRecoveryBudgetStartsClockAtFailureAndNeverReplacesExistingBudget(t *testing.T) {
	ctx := WithRequestRecoveryBudget(context.Background(), 4, 20*time.Second)
	b := RecoveryBudgetFromContext(ctx)
	if !b.deadline.IsZero() {
		t.Fatal("normal initial generation must not start recovery clock")
	}
	if err := b.consume(ctx, &Auth{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("transient")
	b.recordFailure(failure)
	deadline := b.deadline
	b.recordFailure(failure)
	if !deadline.Equal(b.deadline) {
		t.Fatal("another failure must not reset/extend the recovery window")
	}
	if RecoveryBudgetFromContext(WithRequestRecoveryBudget(ctx, 100, time.Hour)) != b {
		t.Fatal("nested handler replaced the shared budget")
	}
	b.deadline = time.Now().Add(-time.Millisecond)
	if err := b.consume(ctx, &Auth{ID: "b"}); !errors.Is(err, failure) {
		t.Fatalf("exhausted budget returned %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("recovery deadline must not cancel a healthy established stream")
	}
}

func TestRecoveryBudgetRejectsWaitBeyondRecoveryOrClientDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ctx = WithRequestRecoveryBudget(ctx, 4, 20*time.Second)
	b := RecoveryBudgetFromContext(ctx)
	b.recordFailure(errors.New("overload"))
	if b.available(ctx, time.Second) {
		t.Fatal("wait would exceed client deadline")
	}
	b.deadline = time.Now().Add(10 * time.Millisecond)
	if b.available(context.Background(), 20*time.Millisecond) {
		t.Fatal("wait would exceed recovery deadline")
	}
	b.MarkOutput()
	if b.available(context.Background(), 0) {
		t.Fatal("output must permanently disable replay")
	}
}

func TestWaitForRecoveryCooldownStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	start := time.Now()
	if err := waitForCooldown(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancel did not stop cooldown promptly")
	}
}

func TestOverloadRecoveryBackoffEscalatesAndHonorsProviderHint(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a := &Auth{ID: "a"}
	base := time.Now()
	result := Result{AuthID: a.ID, Model: "m", Error: &Error{HTTPStatus: 502, Code: "server_is_overloaded"}}
	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 20 * time.Second} {
		now := base.Add(time.Duration(i) * time.Minute)
		m.mu.Lock()
		m.applyRecoveryTestFailure(a, result, now)
		m.mu.Unlock()
		if got := a.ModelStates["m"].NextRetryAfter.Sub(now); got != want {
			t.Fatalf("failure%d cooldown=%v want%v", i+1, got, want)
		}
	}
	hint := 30 * time.Second
	result.RetryAfter = &hint
	m.mu.Lock()
	m.applyRecoveryTestFailure(a, result, base.Add(5*time.Minute))
	m.mu.Unlock()
	if got := a.ModelStates["m"].NextRetryAfter.Sub(base.Add(5 * time.Minute)); got != hint {
		t.Fatalf("Retry-After ignored: %v", got)
	}
}

func TestHTTP503UsesShortRecoveryWithoutBroadeningUnknown5xx(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504} {
		m := NewManager(nil, nil, nil)
		a := &Auth{ID: "a"}
		now := time.Now()
		m.mu.Lock()
		m.applyRecoveryTestFailure(a, Result{AuthID: a.ID, Model: "m", Error: &Error{HTTPStatus: status, Message: "upstream unavailable"}}, now)
		m.mu.Unlock()
		want := time.Minute
		if status == 503 {
			want = 5 * time.Second
		}
		if got := a.ModelStates["m"].NextRetryAfter.Sub(now); got != want {
			t.Fatalf("status%d cooldown=%v want%v", status, got, want)
		}
	}
}

func (m *Manager) applyRecoveryTestFailure(a *Auth, r Result, now time.Time) {
	newCooldownService(m).applyModelFailureLocked(a, r, now, &resultStateEffects{})
}

func TestOverloadRecoveryOldSuccessCannotEraseNewerCooldown(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a := &Auth{ID: "a"}
	now := time.Now()
	s := newCooldownService(m)
	m.mu.Lock()
	s.applyModelFailureLocked(a, Result{Model: "m", Error: &Error{HTTPStatus: 502, Code: "server_is_overloaded"}}, now, &resultStateEffects{})
	deadline := a.ModelStates["m"].NextRetryAfter
	for _, startedAt := range []time.Time{{}, now.Add(-time.Second)} {
		s.applySuccessLocked(a, Result{Model: "m", Success: true, StartedAt: startedAt}, now.Add(time.Second), &resultStateEffects{})
		if !a.ModelStates["m"].NextRetryAfter.Equal(deadline) {
			t.Fatal("old successful attempt cleared newer overload")
		}
	}
	s.applySuccessLocked(a, Result{Model: "m", Success: true, StartedAt: now.Add(6 * time.Second)}, now.Add(7*time.Second), &resultStateEffects{})
	if !a.ModelStates["m"].NextRetryAfter.IsZero() || m.overloadRecovery[overloadRecoveryKey(a.ID, "m")] != nil {
		t.Fatal("new successful recovery must reset cooldown and streak")
	}
	m.mu.Unlock()
}

func TestOverloadRecoveryAllowsOneProbeAndReleasesOnCancellation(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.overloadRecovery = map[string]*overloadRecoveryState{overloadRecoveryKey("a", "m"): {streak: 1, until: time.Now().Add(-time.Second)}}
	release, ok := m.acquireOverloadProbe("a", "m")
	if !ok {
		t.Fatal("first recovery probe rejected")
	}
	if _, ok := m.acquireOverloadProbe("a", "m"); ok {
		t.Fatal("concurrent recovery probe admitted")
	}
	release()
	oldRelease := release
	release, ok = m.acquireOverloadProbe("a", "m")
	if !ok {
		t.Fatal("probe slot leaked")
	}
	oldRelease()
	if _, ok := m.acquireOverloadProbe("a", "m"); ok {
		t.Fatal("old probe release cleared a newer probe")
	}
	release()
}

func TestRecoveryBudgetCannotRetrySinglePickOrPinnedAuth(t *testing.T) {
	m := NewManager(nil, nil, nil)
	for _, meta := range []map[string]any{{cliproxyexecutor.SinglePickMetadataKey: true}, {cliproxyexecutor.PinnedAuthMetadataKey: "a"}} {
		ctx := WithRequestRecoveryBudget(context.Background(), 4, 20*time.Second)
		failure := &Error{HTTPStatus: 502, Code: "server_is_overloaded"}
		_, err := m.RecoverStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "m"}, cliproxyexecutor.Options{Metadata: meta}, failure)
		if err != failure {
			t.Fatalf("single-pick retry was attempted: %v", err)
		}
	}
}

func TestRecoveryWaitPreservesTenantAndChannelScope(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(3, 30*time.Second)
	model := "scope-recovery-model"
	now := time.Now()
	for _, a := range []*Auth{
		{ID: "allowed", Provider: "codex", Label: "allowed-channel", ModelStates: map[string]*ModelState{model: {Unavailable: true, Status: StatusError, NextRetryAfter: now.Add(5 * time.Second)}}},
		{ID: "wrong-channel", Provider: "codex", Label: "other-channel"},
		{ID: "wrong-tenant", TenantID: "another-tenant", Provider: "codex", Label: "allowed-channel"},
	} {
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	ctx := WithRequestRecoveryBudget(context.Background(), 4, 20*time.Second)
	RecoveryBudgetFromContext(ctx).recordFailure(errors.New("overload"))
	wait, ok := m.recoveryWait(ctx, []string{"codex"}, model, map[string]any{"allowed-channels": []string{"allowed-channel"}}, 30*time.Second)
	if !ok || wait < 4*time.Second || wait > 6*time.Second {
		t.Fatalf("scope leaked to an idle unauthorized candidate: wait=%v ok=%v", wait, ok)
	}
}

func TestRecoveryBudgetPreservesGlobalAndAuthRetryOverrides(t *testing.T) {
	ctx := WithRequestRecoveryBudget(context.Background(), 4, 20*time.Second)
	b := RecoveryBudgetFromContext(ctx)
	a := &Auth{ID: "a", Metadata: map[string]any{"request_retry": float64(0)}}
	if !b.authRetryAllowed(a, 3) {
		t.Fatal("initial call blocked")
	}
	if err := b.consume(ctx, a); err != nil {
		t.Fatal(err)
	}
	if b.authRetryAllowed(a, 3) {
		t.Fatal("credential override0 permitted a repeat")
	}
	bAuth := &Auth{ID: "b"}
	if err := b.consume(ctx, bAuth); err != nil {
		t.Fatal(err)
	}
	if b.authRetryAllowed(bAuth, 0) {
		t.Fatal("global retry0 permitted a repeat")
	}
	if !b.authRetryAllowed(bAuth, 3) {
		t.Fatal("global retry3 blocked permitted repeat")
	}
}

func TestRecoveryBudgetBoundsSaturatedAccountQueueWithoutCancelingExecution(t *testing.T) {
	m := NewManager(nil, nil, nil)
	a := &Auth{ID: "saturated-recovery", Metadata: map[string]any{"concurrency_limit": 1}}
	release, err := m.acquireAccountSlot(a)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx := WithRequestRecoveryBudget(context.Background(), 4, 50*time.Millisecond)
	failure := errors.New("upstream overloaded")
	RecoveryBudgetFromContext(ctx).recordFailure(failure)
	scope := mixedExecutionScope{saturated: []*mixedExecutionCandidate{{auth: a, execCtx: ctx}}}
	started := time.Now()
	_, _, err = newExecutionService(m).waitForSaturatedSlot(ctx, &scope)
	if !errors.Is(err, failure) || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("queue ignored recovery window: err=%v elapsed=%v", err, time.Since(started))
	}
	if ctx.Err() != nil {
		t.Fatal("bounded queue canceled the normal execution context")
	}
}

func TestOverloadRecoveryUsesActualStreamRetryAfterHeader(t *testing.T) {
	m := NewManager(nil, nil, nil)
	if _, err := m.Register(context.Background(), &Auth{ID: "header-recovery", Provider: "codex"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	m.MarkResult(context.Background(), Result{AuthID: "header-recovery", Model: "m", Error: &Error{HTTPStatus: 502, Code: "server_is_overloaded"}, Headers: http.Header{"Retry-After": []string{"30"}}})
	a, _ := m.GetByID("header-recovery")
	if delay := a.ModelStates["m"].NextRetryAfter.Sub(now); delay < 29*time.Second || delay > 31*time.Second {
		t.Fatalf("SSE Retry-After ignored: %v", delay)
	}
	date := now.Add(40 * time.Second).UTC().Format(http.TimeFormat)
	if delay := temporaryUpstreamRetryAfter(http.Header{"Retry-After": []string{date}}, now); delay == nil || *delay < 39*time.Second || *delay > 41*time.Second {
		t.Fatalf("HTTP-date Retry-After ignored: %v", delay)
	}
	for _, value := range []string{"0", "-5", "invalid"} {
		if delay := temporaryUpstreamRetryAfter(http.Header{"Retry-After": []string{value}}, now); delay != nil {
			t.Fatalf("invalid Retry-After %q accepted: %v", value, delay)
		}
	}
}

func TestCanceledRecoveryStreamDoesNotClearOverloadHealth(t *testing.T) {
	for _, format := range []sdktranslator.Format{sdktranslator.FromString("openai"), sdktranslator.FormatOpenAIResponse} {
		t.Run(format.String(), func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			a := &Auth{ID: "cancel-recovery", Provider: "codex"}
			if _, err := m.Register(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Model: "m", Error: &Error{HTTPStatus: 502, Code: "server_is_overloaded"}})
			key := overloadRecoveryKey(a.ID, "m")
			m.mu.Lock()
			state := m.overloadRecovery[key]
			state.until = time.Now().Add(-time.Millisecond)
			m.auths[a.ID].ModelStates["m"].NextRetryAfter = state.until
			before := m.auths[a.ID].ModelStates["m"].NextRetryAfter
			failedAt := state.failedAt
			m.mu.Unlock()
			releaseProbe, ok := m.acquireOverloadProbe(a.ID, "m")
			if !ok {
				t.Fatal("recovery probe was not admitted")
			}
			startedAt := time.Now()
			ctx, cancel := context.WithCancel(context.Background())
			chunks := make(chan cliproxyexecutor.StreamChunk)
			stream := newExecutionService(m).wrapStreamResult(ctx, a, "codex", "m", format, &cliproxyexecutor.StreamResult{Chunks: chunks}, releaseProbe, startedAt)
			cancel()
			close(chunks) // Provider observes cancellation and exits without Err.
			for range stream.Chunks {
			}
			m.mu.RLock()
			defer m.mu.RUnlock()
			current := m.overloadRecovery[key]
			if current == nil || current.streak != 1 || !current.failedAt.Equal(failedAt) {
				t.Fatal("canceled generation was treated as successful upstream recovery")
			}
			if current.probeInFlight {
				t.Fatal("cancellation leaked the half-open probe")
			}
			model := m.auths[a.ID].ModelStates["m"]
			if model.LastError == nil || model.LastError.Code != "server_is_overloaded" || !model.NextRetryAfter.Equal(before) {
				t.Fatalf("cancellation replaced overload health: %+v", model)
			}
		})
	}
}
