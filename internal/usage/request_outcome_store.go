package usage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func ensureRequestOutcomeSchema(db *sql.DB) error {
	for _, column := range []string{"request_id TEXT NOT NULL DEFAULT ''", "attempt BIGINT NOT NULL DEFAULT 0", "failure_stage TEXT NOT NULL DEFAULT ''",
		"upstream_protocol TEXT NOT NULL DEFAULT ''", "upstream_transport_group TEXT NOT NULL DEFAULT ''", "upstream_transport_kind TEXT NOT NULL DEFAULT ''",
		"upstream_connection_id TEXT NOT NULL DEFAULT ''", "upstream_connection_reused BOOLEAN", "upstream_error_stage TEXT NOT NULL DEFAULT ''"} {
		if _, err := db.Exec("ALTER TABLE request_logs ADD COLUMN " + column); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return fmt.Errorf("usage: add request outcome metadata: %w", err)
		}
	}
	_, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_request_logs_tenant_request_id ON request_logs(tenant_id, request_id) WHERE request_id <> '';
	CREATE TABLE IF NOT EXISTS request_outcomes (
	 tenant_id TEXT NOT NULL, request_id TEXT NOT NULL, timestamp TEXT NOT NULL, completed_at TEXT NOT NULL,
	 api_key TEXT NOT NULL DEFAULT '', api_key_id TEXT NOT NULL DEFAULT '', api_key_name TEXT NOT NULL DEFAULT '',
	 model TEXT NOT NULL DEFAULT '', auth_index TEXT NOT NULL DEFAULT '', auth_subject_id TEXT NOT NULL DEFAULT '',
	 channel_name TEXT NOT NULL DEFAULT '', result TEXT NOT NULL, failure_stage TEXT NOT NULL DEFAULT '',
	 attempts BIGINT NOT NULL DEFAULT 0, failed_attempts BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(tenant_id, request_id));
	CREATE INDEX IF NOT EXISTS idx_request_outcomes_tenant_time ON request_outcomes(tenant_id, timestamp);`)
	if err != nil {
		return err
	}
	for _, column := range []string{"response_status INTEGER NOT NULL DEFAULT 0", "error_status INTEGER NOT NULL DEFAULT 0", "error_code TEXT NOT NULL DEFAULT ''", "error_type TEXT NOT NULL DEFAULT ''", "error_reason TEXT NOT NULL DEFAULT ''", "error_phase TEXT NOT NULL DEFAULT ''"} {
		if _, err := db.Exec("ALTER TABLE request_outcomes ADD COLUMN " + column); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return fmt.Errorf("usage: add terminal diagnostics: %w", err)
		}
	}
	return nil
}

func insertRequestOutcomeTx(tx usageWriteTx, plan requestLogWritePlan) error {
	o := plan.entry.RequestOutcome
	if o == nil || strings.TrimSpace(o.RequestID) == "" {
		return fmt.Errorf("usage: terminal request record has no identity")
	}
	switch o.Result {
	case coreusage.OutcomeSucceeded, coreusage.OutcomeRecovered, coreusage.OutcomeFailed, coreusage.OutcomeClientCanceled, coreusage.OutcomeClientDeliveryFailed, coreusage.OutcomeUnknown:
	default:
		return fmt.Errorf("usage: invalid request outcome %q", o.Result)
	}
	_, err := tx.Exec(`INSERT INTO request_outcomes
	(tenant_id, request_id, timestamp, completed_at, api_key, api_key_id, api_key_name, model, auth_index, auth_subject_id, channel_name, result, failure_stage, attempts, failed_attempts, response_status, error_status, error_code, error_type, error_reason, error_phase)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(tenant_id, request_id) DO NOTHING`, plan.tenantID, o.RequestID, o.StartedAt.UTC().Format(time.RFC3339Nano), o.CompletedAt.UTC().Format(time.RFC3339Nano),
		plan.entry.APIKey, plan.entry.APIKeyID, plan.entry.APIKeyName, plan.entry.Model, plan.entry.AuthIndex, plan.entry.AuthSubjectID, plan.entry.ChannelName,
		o.Result, o.FailureStage, o.Attempts, o.FailedAttempts, o.ResponseStatus, o.ErrorStatus, o.ErrorCode, o.ErrorType, o.ErrorReason, o.ErrorPhase)
	return err
}

// RequestOutcomeStats reports lifecycle evidence separately from raw attempts.
// UnknownLegacyAttempts is a row count, not a guessed number of failed requests.
type RequestOutcomeStats struct {
	SucceededRequests            int64 `json:"succeeded_requests"`
	RecoveredRequests            int64 `json:"recovered_requests"`
	FinalFailedRequests          int64 `json:"final_failed_requests"`
	ClientCanceledRequests       int64 `json:"client_canceled_requests"`
	ClientDeliveryFailedRequests int64 `json:"client_delivery_failed_requests"`
	UnknownRequests              int64 `json:"unknown_requests"`
	UnknownLegacyAttempts        int64 `json:"unknown_legacy_attempts"`
	FailedAttempts               int64 `json:"failed_attempts"`
}

// QueryRequestOutcomeStats reuses the exact tenant/key/model/channel predicate
// of the attempt list. A status filter selects attempts, never fabricates a
// terminal failure. A request is counted once even if several attempts match.
func QueryRequestOutcomeStats(params LogQueryParams) (RequestOutcomeStats, error) {
	var stats RequestOutcomeStats
	db := getReadDB()
	if db == nil {
		return stats, nil
	}
	where, args := buildWhereClause(params)
	if err := db.QueryRow(`SELECT COALESCE(SUM(CASE WHEN request_id = '' THEN 1 ELSE 0 END), 0), COALESCE(SUM(CASE WHEN failed <> 0 THEN 1 ELSE 0 END), 0)
	FROM request_logs`+where, args...).Scan(&stats.UnknownLegacyAttempts, &stats.FailedAttempts); err != nil {
		return stats, err
	}
	rows, err := db.Query(`SELECT COALESCE(outcome.result, 'unknown'), COUNT(*)
	FROM (SELECT tenant_id, request_id FROM request_logs`+where+` AND request_id <> '' GROUP BY tenant_id, request_id) selected
	LEFT JOIN request_outcomes outcome ON outcome.tenant_id = selected.tenant_id AND outcome.request_id = selected.request_id
	GROUP BY COALESCE(outcome.result, 'unknown')`, args...)
	if err != nil {
		return stats, err
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		var count int64
		if err := rows.Scan(&result, &count); err != nil {
			return stats, err
		}
		switch result {
		case coreusage.OutcomeSucceeded:
			stats.SucceededRequests += count
		case coreusage.OutcomeRecovered:
			stats.RecoveredRequests += count
		case coreusage.OutcomeFailed:
			stats.FinalFailedRequests += count
		case coreusage.OutcomeClientCanceled:
			stats.ClientCanceledRequests += count
		case coreusage.OutcomeClientDeliveryFailed:
			stats.ClientDeliveryFailedRequests += count
		default:
			stats.UnknownRequests += count
		}
	}
	return stats, rows.Err()
}
