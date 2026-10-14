package usage

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestTerminalDiagnosticsSurviveSpoolPurgeAndAPIQuery(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{StoreContent: true})
	now := time.Now().UTC()
	d := coreusage.RequestFailure{ResponseStatus: 200, ErrorStatus: 507, ErrorCode: "upstream_request_buffer_limit", ErrorType: "upstream_error", ErrorReason: "upstream_retry_buffer_exhausted", ErrorPhase: "upstream"}
	entry := normalizeRequestLogEntry(RequestLogEntry{APIKey: "key", Model: "test", Timestamp: now, RequestOutcome: &coreusage.RequestOutcome{RequestFailure: d, RequestID: "diagnostic", StartedAt: now, CompletedAt: now, Result: coreusage.OutcomeFailed}})
	line, err := encodeUsageSpoolRecord(entry, usageSpoolReasonWriteFailed, time.Time{}, now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := decodeUsageSpoolRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RequestOutcome == nil || decoded.RequestOutcome.RequestFailure != d {
		t.Fatalf("spool=%+v", decoded.RequestOutcome)
	}
	InsertRequestLog(decoded)
	if _, err = ClearRequestLogs(ClearRequestLogsOptions{ClearBodyContent: true, ClearDetailContent: true}); err != nil {
		t.Fatal(err)
	}
	page, err := QueryRequestOutcomes(LogQueryParams{Days: 1, APIKeys: []string{"key"}}, nil)
	if err != nil || page.Total != 1 || page.Items[0].RequestFailure != d || page.Items[0].Attempts != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}
func TestTerminalDiagnosticsMigrateLegacySchemaWithoutGuessingCause(t *testing.T) {
	initTestUsageDB(t, config.RequestLogStorageConfig{})
	db := getDB()
	for _, column := range []string{"response_status", "error_status", "error_code", "error_type", "error_reason", "error_phase"} {
		if _, err := db.Exec("ALTER TABLE request_outcomes DROP COLUMN " + column); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureRequestOutcomeSchema(db); err != nil {
		t.Fatal(err)
	}
	if err := ensureRequestOutcomeSchema(db); err != nil {
		t.Fatal(err)
	}
	insertOutcomeForTest("legacy", coreusage.OutcomeFailed, coreusage.FailureBeforeOutput, "key", 0, 0)
	page, err := QueryRequestOutcomes(LogQueryParams{Days: 1}, nil)
	if err != nil || page.Total != 1 || page.Items[0].RequestFailure != (coreusage.RequestFailure{}) {
		t.Fatalf("legacy cause invented: page=%+v err=%v", page, err)
	}
}
