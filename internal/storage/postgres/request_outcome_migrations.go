package postgres

// Metadata is independent of compressed request bodies and is never inferred
// from the absence of a successful upstream attempt.
const requestOutcomeSQL = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS attempt BIGINT NOT NULL DEFAULT 0;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS failure_stage TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_protocol TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_transport_group TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_transport_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_connection_id TEXT NOT NULL DEFAULT '';
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_connection_reused BOOLEAN;
ALTER TABLE request_logs ADD COLUMN IF NOT EXISTS upstream_error_stage TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_request_logs_tenant_request_id ON request_logs(tenant_id, request_id) WHERE request_id <> '';
CREATE TABLE IF NOT EXISTS request_outcomes (
 tenant_id UUID NOT NULL,
 request_id TEXT NOT NULL,
 timestamp TIMESTAMPTZ NOT NULL,
 completed_at TIMESTAMPTZ NOT NULL,
 api_key TEXT NOT NULL DEFAULT '',
 api_key_id TEXT NOT NULL DEFAULT '',
 api_key_name TEXT NOT NULL DEFAULT '',
 model TEXT NOT NULL DEFAULT '',
 auth_index TEXT NOT NULL DEFAULT '',
 auth_subject_id TEXT NOT NULL DEFAULT '',
 channel_name TEXT NOT NULL DEFAULT '',
 result TEXT NOT NULL,
 failure_stage TEXT NOT NULL DEFAULT '',
 attempts BIGINT NOT NULL DEFAULT 0,
 failed_attempts BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (tenant_id, request_id)
);
CREATE INDEX IF NOT EXISTS idx_request_outcomes_tenant_time ON request_outcomes(tenant_id, timestamp);
`
