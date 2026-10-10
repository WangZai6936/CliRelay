package usage

import (
	"context"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"strings"
	"time"
)

const logicalRequestCacheLimit = 32768

type logicalRequestState struct {
	at        time.Time
	success   bool
	failed    bool
	recovered bool
	canceled  bool
	counted   bool
	statsKey  string
	model     string
	countedAt time.Time
}

// countLogicalRequestLocked correlates provider attempts of the same incoming
// request. Raw attempt logs and token accounting are retained independently.
// A successful retry wins even when queue/spool replay changes delivery order.
func (s *RequestStatistics) countLogicalRequestLocked(statsKey, model string, record coreusage.Record, at time.Time, success bool) bool {
	requestID := record.RequestID
	if record.RequestCorrelationID != "" {
		requestID = record.RequestCorrelationID
	}
	attempts := record.ProviderAttempts
	if attempts < 1 {
		attempts = 1
	}
	s.attemptCount += attempts
	if !success {
		s.failedAttemptCount += attempts
	} else if attempts > 1 {
		s.failedAttemptCount += attempts - 1
	}
	if strings.TrimSpace(requestID) == "" {
		return true // Legacy/internal records have no correlation identity.
	}
	if s.logicalRequests == nil {
		s.logicalRequests = make(map[string]logicalRequestState)
	}
	identity := record.TrustedTenantID + "\x00" + statsKey + "\x00" + record.APIIdentifier + "\x00" + requestID
	state, exists := s.logicalRequests[identity]
	if !exists && len(s.logicalRequests) >= logicalRequestCacheLimit {
		delete(s.logicalRequests, s.logicalRequestOrder[s.logicalRequestCursor])
		s.logicalRequestOrder[s.logicalRequestCursor] = identity
		s.logicalRequestCursor = (s.logicalRequestCursor + 1) % logicalRequestCacheLimit
	} else if !exists {
		s.logicalRequestOrder = append(s.logicalRequestOrder, identity)
	}
	if state.canceled && !success {
		state.failed = true
		s.logicalRequests[identity] = state
		return false
	}
	if state.canceled && success {
		s.canceledCount--
		state.canceled = false
	}
	if state.counted && !state.success && success {
		s.failureCount--
		s.successCount++
	}
	state.at = at
	newRequest := !state.counted
	if newRequest {
		state.counted = true
		state.statsKey, state.model, state.countedAt = statsKey, model, at
	}
	state.success = state.success || success
	state.failed = state.failed || !success || attempts > 1
	if state.success && state.failed && !state.recovered {
		state.recovered = true
		s.recoveredCount++
	}
	s.logicalRequests[identity] = state
	return newRequest
}

// RecordRequestCancellation keeps client cancellation separate from provider
// failures. No billing/attempt log is fabricated for a canceled generation.
func RecordRequestCancellation(ctx context.Context, record coreusage.Record) {
	if !statisticsEnabled.Load() || defaultRequestStatistics == nil {
		return
	}
	defaultRequestStatistics.recordCancellation(record)
}

func (s *RequestStatistics) recordCancellation(record coreusage.Record) {
	statsKey := record.APIKey
	if statsKey == "" {
		statsKey = record.APIIdentifier
	}
	requestID := record.RequestCorrelationID
	if requestID == "" {
		requestID = record.RequestID
	}
	if requestID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := record.TrustedTenantID + "\x00" + statsKey + "\x00" + record.APIIdentifier + "\x00" + requestID
	if s.logicalRequests == nil {
		s.logicalRequests = make(map[string]logicalRequestState)
	}
	state, exists := s.logicalRequests[identity]
	if state.success || state.canceled {
		return
	}
	if !exists {
		if len(s.logicalRequests) >= logicalRequestCacheLimit {
			delete(s.logicalRequests, s.logicalRequestOrder[s.logicalRequestCursor])
			s.logicalRequestOrder[s.logicalRequestCursor] = identity
			s.logicalRequestCursor = (s.logicalRequestCursor + 1) % logicalRequestCacheLimit
		} else {
			s.logicalRequestOrder = append(s.logicalRequestOrder, identity)
		}
	}
	if state.counted {
		s.totalRequests--
		s.failureCount--
		s.requestsByDay[state.countedAt.Format("2006-01-02")]--
		s.requestsByHour[state.countedAt.Hour()]--
		if api := s.apis[state.statsKey]; api != nil {
			api.TotalRequests--
			if model := api.Models[state.model]; model != nil {
				model.TotalRequests--
			}
		}
		state.counted = false
	}
	state.canceled = true
	state.at = record.RequestedAt
	s.logicalRequests[identity] = state
	s.canceledCount++
}
