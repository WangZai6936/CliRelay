package auth

import (
	"time"
)

type overloadRecoveryState struct {
	streak          int
	failedAt        time.Time
	until           time.Time
	probeInFlight   bool
	probeGeneration uint64
}

func overloadRecoveryKey(authID, model string) string { return authID + "\x00" + model }

// recordOverloadLocked progressively backs off repeated load shedding. The
// streak survives cooldown expiry; only a sufficiently new successful attempt
// establishes that the provider recovered. Callers hold Manager.mu.
func (m *Manager) recordOverloadLocked(auth *Auth, result Result, now time.Time) time.Duration {
	if m == nil {
		return transientUpstreamOverloadCooldown
	}
	if m.overloadRecovery == nil {
		m.overloadRecovery = make(map[string]*overloadRecoveryState)
	}
	key := overloadRecoveryKey(auth.ID, result.Model)
	state := m.overloadRecovery[key]
	if state == nil {
		state = &overloadRecoveryState{}
		m.overloadRecovery[key] = state
	}
	state.streak = min(state.streak+1, 3)
	state.failedAt = now
	state.probeInFlight = false
	state.probeGeneration++
	cooldown := transientUpstreamOverloadCooldown * time.Duration(1<<(state.streak-1))
	if result.RetryAfter != nil && *result.RetryAfter > cooldown {
		cooldown = *result.RetryAfter
	}
	state.until = now.Add(cooldown)
	return cooldown
}

func (m *Manager) clearOverloadOnSuccessLocked(auth *Auth, result Result) bool {
	if m == nil {
		return true
	}
	key := overloadRecoveryKey(auth.ID, result.Model)
	state := m.overloadRecovery[key]
	if state == nil {
		return true
	}
	// Results from callers without an attempt start timestamp cannot prove they
	// postdate the load-shed. Preserve that cooldown rather than guessing.
	if result.StartedAt.IsZero() || !result.StartedAt.After(state.failedAt) {
		return false
	}
	delete(m.overloadRecovery, key)
	return true
}

// acquireOverloadProbe permits one recovering request per auth/model. The
// regular selector blocks the cooldown itself; this gate prevents a herd of
// requests at its expiry. It is released on any attempt exit or cancellation.
func (m *Manager) acquireOverloadProbe(authID, model string) (func(), bool) {
	m.mu.Lock()
	state := m.overloadRecovery[overloadRecoveryKey(authID, model)]
	if state == nil {
		m.mu.Unlock()
		return func() {}, true
	}
	if state.until.After(time.Now()) || state.probeInFlight {
		m.mu.Unlock()
		return nil, false
	}
	state.probeInFlight = true
	state.probeGeneration++
	generation := state.probeGeneration
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		if current := m.overloadRecovery[overloadRecoveryKey(authID, model)]; current == state && current.probeGeneration == generation {
			current.probeInFlight = false
		}
		m.mu.Unlock()
	}, true
}
