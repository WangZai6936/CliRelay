package usage

// The second branch is evidence of a known request identity without a terminal
// record (still running, process loss, or queue recovery). It is always unknown.
// Window functions retain the last attempt's display metadata while counting
// the whole correlated chain in the selected tenant/time range.
const requestOutcomeReadModelSQL = `(
 SELECT tenant_id, request_id, timestamp, completed_at, api_key, api_key_id, api_key_name, model,
 auth_index, auth_subject_id, channel_name, result, failure_stage, attempts, failed_attempts, response_status, error_status, error_code, error_type, error_reason, error_phase FROM request_outcomes
 UNION ALL
 SELECT tenant_id, request_id, first_at AS timestamp, NULL AS completed_at, api_key, api_key_id, api_key_name, model,
 auth_index, auth_subject_id, channel_name, 'unknown' AS result, '' AS failure_stage, chain_attempts AS attempts, chain_failed AS failed_attempts, 0 AS response_status, 0 AS error_status, '' AS error_code, '' AS error_type, '' AS error_reason, '' AS error_phase
 FROM (
  SELECT logs.*,
   MIN(timestamp) OVER (PARTITION BY tenant_id, request_id) AS first_at,
   MAX(attempt) OVER (PARTITION BY tenant_id, request_id) AS chain_attempts,
   SUM(CASE WHEN failed <> 0 THEN 1 ELSE 0 END) OVER (PARTITION BY tenant_id, request_id) AS chain_failed,
   ROW_NUMBER() OVER (PARTITION BY tenant_id, request_id ORDER BY attempt DESC, id DESC) AS position
  FROM request_logs logs
  WHERE tenant_id = ? AND timestamp >= ? AND request_id <> ''
   AND NOT EXISTS (SELECT 1 FROM request_outcomes terminal WHERE terminal.tenant_id = logs.tenant_id AND terminal.request_id = logs.request_id)
 ) unmatched WHERE position = 1
) request_evidence`
