package usage

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func insertOutcomeForTest(id, result, stage, key string, attempts, failed int64) {
	now := time.Now().UTC()
	InsertRequestLog(RequestLogEntry{APIKey: key, Model: "test-model", Timestamp: now,
		RequestOutcome: &coreusage.RequestOutcome{RequestID: id, StartedAt: now, CompletedAt: now,
			Result: result, FailureStage: stage, Attempts: attempts, FailedAttempts: failed}})
}

func TestRequestOutcomeSurvivesBodyAndDetailPurgeWithoutExtraBillingRow(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{StoreContent: true})
	now := time.Now().UTC()
	for i := int64(1); i <= 3; i++ {
		InsertRequestLog(RequestLogEntry{APIKey: "key", Model: "test-model", Timestamp: now, RequestID: "request-retry", Attempt: i,
			Failed: i < 3, Tokens: TokenStats{InputTokens: 2, TotalTokens: 2}, InputContent: `{"input":"test"}`, DetailContent: `{"request_id":"old-detail"}`})
	}
	insertOutcomeForTest("request-retry", coreusage.OutcomeRecovered, "", "key", 3, 2)
	if _, err := ClearRequestLogs(ClearRequestLogsOptions{ClearBodyContent: true, ClearDetailContent: true}); err != nil {
		t.Fatal(err)
	}
	logs, err := QueryLogs(LogQueryParams{Days: 1})
	if err != nil {
		t.Fatal(err)
	}
	if logs.Total != 3 {
		t.Fatalf("terminal record fabricated billing row: %d", logs.Total)
	}
	for _, row := range logs.Items {
		if row.RequestID != "request-retry" || row.Attempt == 0 || row.RequestOutcome != coreusage.OutcomeRecovered {
			t.Fatalf("lost compact metadata: %+v", row)
		}
	}
	stats, err := QueryRequestOutcomeStats(LogQueryParams{Days: 1, Statuses: []string{"failed"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.RecoveredRequests != 1 || stats.FinalFailedRequests != 0 || stats.FailedAttempts != 2 {
		t.Fatalf("attempt failures treated as final failure: %+v", stats)
	}
	var tokens int64
	if err := getDB().QueryRow("SELECT SUM(total_tokens) FROM request_logs").Scan(&tokens); err != nil || tokens != 6 {
		t.Fatalf("tokens = %d, err %v", tokens, err)
	}
}

func TestRequestOutcomeSeparatesFinalFailureCancellationAndUnknownHistory(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	now := time.Now().UTC()
	for _, id := range []string{"failure", "cancel", "missing-terminal", ""} {
		InsertRequestLog(RequestLogEntry{APIKey: "key", Model: "test-model", Timestamp: now, RequestID: id, Attempt: 1, Failed: true})
	}
	insertOutcomeForTest("failure", coreusage.OutcomeFailed, coreusage.FailureBeforeOutput, "key", 1, 1)
	insertOutcomeForTest("cancel", coreusage.OutcomeClientCanceled, coreusage.FailureAfterOutput, "key", 1, 1)
	stats, err := QueryRequestOutcomeStats(LogQueryParams{Days: 1})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FinalFailedRequests != 1 || stats.ClientCanceledRequests != 1 || stats.UnknownRequests != 1 || stats.UnknownLegacyAttempts != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	page, err := QueryRequestOutcomes(LogQueryParams{Days: 1, APIKeys: []string{"key"}}, []string{coreusage.OutcomeClientCanceled})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].FailureStage != coreusage.FailureAfterOutput {
		t.Fatalf("page = %+v", page)
	}
}

func TestRequestOutcomeSpoolReplayKeepsMetadataAndTerminalIdempotency(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	now := time.Now().UTC()
	entry := normalizeRequestLogEntry(RequestLogEntry{APIKey: "key", RequestID: "request", Attempt: 2, FailureStage: coreusage.FailureBeforeOutput, Failed: true, Timestamp: now})
	line, err := encodeUsageSpoolRecord(entry, usageSpoolReasonQueueFull, time.Time{}, now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := decodeUsageSpoolRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RequestID != "request" || decoded.Attempt != 2 || decoded.FailureStage != coreusage.FailureBeforeOutput {
		t.Fatalf("decoded = %+v", decoded)
	}
	InsertRequestLog(decoded)
	entry.RequestOutcome = &coreusage.RequestOutcome{RequestID: "request", Result: coreusage.OutcomeFailed, StartedAt: now, CompletedAt: now, Attempts: 2, FailedAttempts: 2}
	entry.IdempotencyKey = coreusage.NewIdempotencyKey()
	line, err = encodeUsageSpoolRecord(entry, usageSpoolReasonWriteFailed, time.Time{}, now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err = decodeUsageSpoolRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	InsertRequestLog(decoded)
	InsertRequestLog(decoded)
	page, err := QueryRequestOutcomes(LogQueryParams{Days: 1}, nil)
	if err != nil || page.Total != 1 || page.Items[0].Result != coreusage.OutcomeFailed {
		t.Fatalf("page = %+v, err = %v", page, err)
	}
	logs, err := QueryLogs(LogQueryParams{Days: 1})
	if err != nil || logs.Total != 1 {
		t.Fatalf("logs = %+v, err = %v", logs, err)
	}
}

func TestRequestOutcomeTenantAndAPIKeyIsolation(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	now := time.Now().UTC()
	for _, tenant := range []string{identity.SystemTenantID, "22222222-2222-2222-2222-222222222222"} {
		for _, key := range []string{"key-a", "key-b"} {
			InsertRequestLog(RequestLogEntry{TrustedTenantID: tenant, APIKey: key, Timestamp: now, RequestOutcome: &coreusage.RequestOutcome{RequestID: tenant + key, Result: coreusage.OutcomeFailed, StartedAt: now, CompletedAt: now, Attempts: 1, FailedAttempts: 1}})
		}
	}
	page, err := QueryRequestOutcomes(LogQueryParams{TenantID: identity.SystemTenantID, Days: 1, APIKeys: []string{"key-a"}}, nil)
	if err != nil || page.Total != 1 || page.Stats.FinalFailedRequests != 1 {
		t.Fatalf("tenant/key filter leaked: %+v, err = %v", page, err)
	}
}

func TestRequestOutcomeRetentionAndExplicitRecordClear(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	now := time.Now().UTC()
	for _, at := range []time.Time{now.AddDate(0, 0, -10), now} {
		InsertRequestLog(RequestLogEntry{Timestamp: at, RequestOutcome: &coreusage.RequestOutcome{RequestID: at.String(), Result: coreusage.OutcomeSucceeded, StartedAt: at, CompletedAt: at}})
	}
	cfg := currentRequestLogStorageConfig()
	cfg.RetentionDays = 7
	if err := cleanupExpiredRequestOutcomes(context.Background(), getDB(), cfg, 1, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := getDB().QueryRow("SELECT COUNT(*) FROM request_outcomes").Scan(&count); err != nil || count != 1 {
		t.Fatalf("remaining = %d, err %v", count, err)
	}
	if _, err := ClearRequestLogs(ClearRequestLogsOptions{ClearRequestRecords: true}); err != nil {
		t.Fatal(err)
	}
	if err := getDB().QueryRow("SELECT COUNT(*) FROM request_outcomes").Scan(&count); err != nil || count != 0 {
		t.Fatalf("clear remaining = %d, err %v", count, err)
	}
}

func TestRequestOutcomeMissingTerminalIsVisibleAsUnknown(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	now := time.Now().UTC()
	for attempt := int64(1); attempt <= 2; attempt++ {
		InsertRequestLog(RequestLogEntry{APIKey: "key", Model: "test-model", Timestamp: now, RequestID: "unmatched", Attempt: attempt, Failed: true})
	}
	page, err := QueryRequestOutcomes(LogQueryParams{Days: 1, Models: []string{"test-model"}}, []string{coreusage.OutcomeUnknown})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Stats.UnknownRequests != 1 || page.Stats.FinalFailedRequests != 0 || page.Items[0].CompletedAt != nil || page.Items[0].Attempts != 2 {
		t.Fatalf("unknown evidence = %+v", page)
	}
	insertOutcomeForTest("unmatched", coreusage.OutcomeFailed, coreusage.FailureBeforeOutput, "key", 2, 2)
	page, err = QueryRequestOutcomes(LogQueryParams{Days: 1}, nil)
	if err != nil || page.Total != 1 || page.Stats.FinalFailedRequests != 1 || page.Stats.UnknownRequests != 0 {
		t.Fatalf("terminal did not replace unknown: %+v, err %v", page, err)
	}
}

func TestRequestOutcomeTransportMetadataSurvivesSpoolAndBodyPurge(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{StoreContent: true})
	now := time.Now().UTC()
	for _, reused := range []*bool{nil, new(bool), func() *bool { b := true; return &b }()} {
		record := coreusage.Record{APIKey: "key", Model: "test-model", RequestedAt: now, ResponseStatus: 200,
			UpstreamProtocol: "HTTP/2.0", UpstreamTransportGroup: "http2_control", UpstreamTransportKind: "tls_fingerprint",
			UpstreamConnectionID: "connection", UpstreamConnectionReused: reused, UpstreamErrorStage: "stream_body",
			InputContent: "private-request", OutputContent: "private-response", DetailContent: `{"old":"details"}`}
		entry := normalizeRequestLogEntry(NewRequestStatistics().ingest(context.Background(), record))
		line, err := encodeUsageSpoolRecord(entry, usageSpoolReasonQueueFull, time.Time{}, now)
		if err != nil {
			t.Fatal(err)
		}
		decoded, _, err := decodeUsageSpoolRecord(line)
		if err != nil {
			t.Fatal(err)
		}
		InsertRequestLog(decoded)
	}
	if _, err := ClearRequestLogs(ClearRequestLogsOptions{ClearBodyContent: true, ClearDetailContent: true}); err != nil {
		t.Fatal(err)
	}
	page, err := QueryLogs(LogQueryParams{Days: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 {
		t.Fatalf("metadata rows = %d", len(page.Items))
	}
	var missing, fresh, reused int
	for _, row := range page.Items {
		if row.UpstreamProtocol != "HTTP/2.0" || row.UpstreamTransportGroup != "http2_control" || row.UpstreamTransportKind != "tls_fingerprint" || row.UpstreamConnectionID != "connection" || row.UpstreamErrorStage != "stream_body" {
			t.Fatalf("metadata lost: %+v", row)
		}
		if row.UpstreamConnectionReused == nil {
			missing++
		} else if *row.UpstreamConnectionReused {
			reused++
		} else {
			fresh++
		}
	}
	if missing != 1 || fresh != 1 || reused != 1 {
		t.Fatalf("nullable reuse metadata: missing=%d fresh=%d reused=%d", missing, fresh, reused)
	}
}
