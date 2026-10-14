package usage

import (
	"fmt"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// RequestOutcomeRow contains client request results without prompts, response
// bodies, credential secrets, or dependence on compressed log content.
type RequestOutcomeRow struct {
	coreusage.RequestFailure
	RequestID      string     `json:"request_id"`
	Timestamp      time.Time  `json:"timestamp"`
	CompletedAt    *time.Time `json:"completed_at"`
	APIKeyID       string     `json:"api_key_id"`
	APIKeyName     string     `json:"api_key_name"`
	Model          string     `json:"model"`
	Result         string     `json:"result"`
	FailureStage   string     `json:"failure_stage,omitempty"`
	Attempts       int64      `json:"attempts"`
	FailedAttempts int64      `json:"failed_attempts"`
}

type RequestOutcomeQueryResult struct {
	Items []RequestOutcomeRow `json:"items"`
	Total int64               `json:"total"`
	Page  int                 `json:"page"`
	Size  int                 `json:"size"`
	Stats RequestOutcomeStats `json:"stats"`
}

// QueryRequestOutcomes lists explicit lifecycle outcomes. Legacy attempt rows
// are reported as unknown evidence rather than synthesized terminal failures.
func QueryRequestOutcomes(params LogQueryParams, results []string) (RequestOutcomeQueryResult, error) {
	if params.Page < 1 {
		params.Page = 1
	}
	if params.Size < 1 {
		params.Size = 50
	}
	if params.Size > 500 {
		params.Size = 500
	}
	params.Status, params.Statuses = "", nil
	params = normalizeLogQueryParams(params)
	result := RequestOutcomeQueryResult{Items: make([]RequestOutcomeRow, 0), Page: params.Page, Size: params.Size}
	db := getReadDB()
	if db == nil {
		return result, nil
	}
	where, args := buildWhereClause(params)
	sourceArgs := []any{params.TenantID, CutoffStartUTC(params.Days).Format(time.RFC3339)}
	valid := make([]any, 0, len(results))
	for _, status := range results {
		switch strings.TrimSpace(status) {
		case coreusage.OutcomeSucceeded, coreusage.OutcomeRecovered, coreusage.OutcomeFailed, coreusage.OutcomeClientCanceled, coreusage.OutcomeClientDeliveryFailed, coreusage.OutcomeUnknown:
			valid = append(valid, strings.TrimSpace(status))
		default:
			return result, fmt.Errorf("usage: invalid result filter %q", status)
		}
	}
	if len(valid) > 0 {
		where += " AND result IN (" + strings.TrimSuffix(strings.Repeat("?,", len(valid)), ",") + ")"
		args = append(args, valid...)
	}
	readArgs := append(append([]any{}, sourceArgs...), args...)
	if err := db.QueryRow("SELECT COUNT(*) FROM "+requestOutcomeReadModelSQL+where, readArgs...).Scan(&result.Total); err != nil {
		return result, err
	}
	rows, err := db.Query(`SELECT request_id, timestamp, completed_at, api_key_id, api_key_name, model, result, failure_stage, attempts, failed_attempts, response_status, error_status, error_code, error_type, error_reason, error_phase FROM `+requestOutcomeReadModelSQL+where+` ORDER BY timestamp DESC LIMIT ? OFFSET ?`, append(readArgs, params.Size, (params.Page-1)*params.Size)...)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var row RequestOutcomeRow
		var start, completed storedTime
		if err := rows.Scan(&row.RequestID, &start, &completed, &row.APIKeyID, &row.APIKeyName, &row.Model, &row.Result, &row.FailureStage, &row.Attempts, &row.FailedAttempts, &row.ResponseStatus, &row.ErrorStatus, &row.ErrorCode, &row.ErrorType, &row.ErrorReason, &row.ErrorPhase); err != nil {
			rows.Close()
			return result, err
		}
		row.Timestamp = start.Time
		if completed.Valid {
			at := completed.Time
			row.CompletedAt = &at
		}
		result.Items = append(result.Items, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	// Summary uses the same filters as the page, including explicit result.
	rows, err = db.Query(`SELECT result, COUNT(*), COALESCE(SUM(failed_attempts), 0) FROM `+requestOutcomeReadModelSQL+where+` GROUP BY result`, readArgs...)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var status string
		var count, failures int64
		if err := rows.Scan(&status, &count, &failures); err != nil {
			rows.Close()
			return result, err
		}
		result.Stats.FailedAttempts += failures
		switch status {
		case coreusage.OutcomeSucceeded:
			result.Stats.SucceededRequests += count
		case coreusage.OutcomeRecovered:
			result.Stats.RecoveredRequests += count
		case coreusage.OutcomeFailed:
			result.Stats.FinalFailedRequests += count
		case coreusage.OutcomeClientCanceled:
			result.Stats.ClientCanceledRequests += count
		case coreusage.OutcomeClientDeliveryFailed:
			result.Stats.ClientDeliveryFailedRequests += count
		default:
			result.Stats.UnknownRequests += count
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	legacyWhere, legacyArgs := buildWhereClause(params)
	if err := db.QueryRow(`SELECT COUNT(*) FROM request_logs`+legacyWhere+` AND request_id = ''`, legacyArgs...).Scan(&result.Stats.UnknownLegacyAttempts); err != nil {
		return result, err
	}
	return result, nil
}
