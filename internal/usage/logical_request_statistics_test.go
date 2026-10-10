package usage

import (
	"context"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestLogicalRequestStatisticsCountsRecoveredRequestOnce(t *testing.T) {
	stats := NewRequestStatistics()
	at := time.Date(2026, 10, 10, 23, 59, 59, 0, time.UTC)
	failure := coreusage.Record{APIKey: "test-key", RequestID: "request-1", Model: "model", RequestedAt: at, Failed: true, ResponseStatus: 500}
	success := failure
	success.Failed = false
	success.ResponseStatus = 200
	success.RequestedAt = at.Add(2 * time.Second)
	success.Detail = coreusage.Detail{InputTokens: 11, OutputTokens: 7, TotalTokens: 18}
	stats.ingest(context.Background(), failure)
	stats.ingest(context.Background(), success)
	snapshot := stats.Snapshot()
	if snapshot.TotalRequests != 1 || snapshot.SuccessCount != 1 || snapshot.FailureCount != 0 || snapshot.AttemptCount != 2 || snapshot.FailedAttemptCount != 1 || snapshot.RecoveredCount != 1 || snapshot.TotalTokens != 18 {
		t.Fatalf("unexpected recovered-request statistics: %+v", snapshot)
	}
	if snapshot.APIs["test-key"].TotalRequests != 1 || snapshot.APIs["test-key"].Models["model"].TotalRequests != 1 {
		t.Fatal("API/model statistics still count provider attempts as requests")
	}
}

func TestLogicalRequestStatisticsSuccessfulOutcomeWinsReplayOrder(t *testing.T) {
	stats := NewRequestStatistics()
	base := coreusage.Record{APIKey: "key", RequestID: "request", Model: "model", ResponseStatus: 200}
	stats.ingest(context.Background(), base)
	base.Failed = true
	base.ResponseStatus = 500
	stats.ingest(context.Background(), base)
	s := stats.Snapshot()
	if s.TotalRequests != 1 || s.SuccessCount != 1 || s.FailureCount != 0 || s.RecoveredCount != 1 {
		t.Fatalf("out-of-order retry was misclassified: %+v", s)
	}
}

func TestLogicalRequestStatisticsScopesIdentityAndCountsInternalRecovery(t *testing.T) {
	stats := NewRequestStatistics()
	base := coreusage.Record{APIKey: "key-a", RequestID: "same-id", Model: "model", ResponseStatus: 200, ProviderAttempts: 2}
	stats.ingest(context.Background(), base)
	base.APIKey = "key-b"
	base.ProviderAttempts = 1
	stats.ingest(context.Background(), base)
	s := stats.Snapshot()
	if s.TotalRequests != 2 || s.AttemptCount != 3 || s.FailedAttemptCount != 1 || s.RecoveredCount != 1 {
		t.Fatalf("identity/recovery accounting is wrong: %+v", s)
	}
}

func TestLogicalRequestCancellationIsSeparateFromFailureAndWinsLateFailedAttempt(t *testing.T) {
	stats := NewRequestStatistics()
	base := coreusage.Record{APIKey: "key", RequestID: "request", Model: "model", Failed: true, ResponseStatus: 500, RequestedAt: time.Now()}
	stats.ingest(context.Background(), base)
	stats.recordCancellation(base)
	stats.recordCancellation(base)
	stats.ingest(context.Background(), base)
	s := stats.Snapshot()
	if s.TotalRequests != 0 || s.FailureCount != 0 || s.CanceledCount != 1 || s.AttemptCount != 2 {
		t.Fatalf("cancellation was counted as failure: %+v", s)
	}
	base.Failed = false
	base.ResponseStatus = 200
	stats.ingest(context.Background(), base)
	s = stats.Snapshot()
	if s.TotalRequests != 1 || s.SuccessCount != 1 || s.CanceledCount != 0 {
		t.Fatalf("late successful completion should win: %+v", s)
	}
}

func TestLogicalRequestStatisticsUsesPrivateCorrelationIdentity(t *testing.T) {
	stats := NewRequestStatistics()
	base := coreusage.Record{APIKey: "key", RequestID: "short-id", RequestCorrelationID: "full-id-a", Model: "model", ResponseStatus: 200}
	stats.ingest(context.Background(), base)
	base.RequestCorrelationID = "full-id-b"
	stats.ingest(context.Background(), base)
	if got := stats.Snapshot().TotalRequests; got != 2 {
		t.Fatalf("short log ID collision merged unrelated requests: %d", got)
	}
}
