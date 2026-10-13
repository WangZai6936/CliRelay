package usage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// cleanupExpiredRequestLogMetadata deletes oldest request_logs past retention
// or hard caps. Cascade removes content. Never touches usage_rollup_buckets or
// ai_account_subject_usage_buckets; shared card totals survive detail retention.
// Skips while rollup backfill marker is incomplete so first upgrade cannot
// prune historical detail before lifetime projection is rebuilt.
func cleanupExpiredRequestLogMetadata(ctx context.Context, db *sql.DB) (int64, error) {
	if db == nil {
		return 0, nil
	}
	cfg := currentRequestLogStorageConfig()
	if !cfg.CleanupEnabled {
		return 0, nil
	}
	if !usageRollupBackfillCompleted(db) {
		return 0, nil
	}
	batch := cfg.CleanupBatchSize
	if batch <= 0 {
		batch = 1000
	}
	maxRuntime := time.Duration(cfg.CleanupMaxRuntimeSeconds) * time.Second
	if maxRuntime <= 0 {
		maxRuntime = 30 * time.Second
	}
	deadline := time.Now().Add(maxRuntime)
	var deletedTotal int64

	for {
		if ctx.Err() != nil {
			return deletedTotal, ctx.Err()
		}
		if time.Now().After(deadline) {
			break
		}
		need, err := metadataCleanupNeeded(db, cfg)
		if err != nil {
			return deletedTotal, err
		}
		if !need {
			break
		}
		n, err := deleteOldestRequestLogsBatch(db, batch)
		if err != nil {
			return deletedTotal, err
		}
		if n == 0 {
			break
		}
		deletedTotal += n
		// Yield briefly so proxy traffic is not starved.
		select {
		case <-ctx.Done():
			return deletedTotal, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := cleanupExpiredRequestOutcomes(ctx, db, cfg, batch, deadline); err != nil {
		return deletedTotal, err
	}
	return deletedTotal, nil
}

func metadataCleanupNeeded(db *sql.DB, cfg requestLogStorageRuntime) (bool, error) {
	if cfg.RetentionDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays).Format(time.RFC3339Nano)
		var exists int
		err := db.QueryRow(`SELECT 1 FROM request_logs WHERE timestamp < ? LIMIT 1`, cutoff).Scan(&exists)
		if err == nil {
			return true, nil
		}
		if err != sql.ErrNoRows {
			return false, fmt.Errorf("usage: check retention cutoff: %w", err)
		}
	}
	if cfg.MaxRows > 0 {
		var count int64
		if err := db.QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&count); err != nil {
			return false, fmt.Errorf("usage: count request_logs: %w", err)
		}
		if count > int64(cfg.MaxRows) {
			return true, nil
		}
	}
	return false, nil
}

func deleteOldestRequestLogsBatch(db *sql.DB, batch int) (int64, error) {
	if batch <= 0 {
		batch = 1000
	}
	// Prefer deleting by retention first when configured.
	cfg := currentRequestLogStorageConfig()
	if cfg.RetentionDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays).Format(time.RFC3339Nano)
		result, err := db.Exec(`
			DELETE FROM request_logs
			WHERE id IN (
				SELECT id FROM request_logs
				WHERE timestamp < ?
				ORDER BY timestamp ASC, id ASC
				LIMIT ?
			)
		`, cutoff, batch)
		if err != nil {
			return 0, fmt.Errorf("usage: delete expired request_logs: %w", err)
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			return n, nil
		}
	}
	if cfg.MaxRows > 0 {
		var count int64
		if err := db.QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&count); err != nil {
			return 0, err
		}
		if count <= int64(cfg.MaxRows) {
			return 0, nil
		}
		// Delete oldest until under cap (one batch).
		result, err := db.Exec(`
			DELETE FROM request_logs
			WHERE id IN (
				SELECT id FROM request_logs
				ORDER BY timestamp ASC, id ASC
				LIMIT ?
			)
		`, batch)
		if err != nil {
			return 0, fmt.Errorf("usage: delete oversized request_logs: %w", err)
		}
		n, _ := result.RowsAffected()
		return n, nil
	}
	return 0, nil
}

func recordCleanupPass(deleted int64, started time.Time, status, errMsg string) {
	db := getDB()
	if db == nil {
		return
	}
	_, _ = db.Exec(`
		CREATE TABLE IF NOT EXISTS request_log_storage_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			metadata_row_count INTEGER NOT NULL DEFAULT 0,
			content_row_count INTEGER NOT NULL DEFAULT 0,
			last_cleanup_started_at TEXT,
			last_cleanup_finished_at TEXT,
			last_cleanup_status TEXT NOT NULL DEFAULT '',
			last_cleanup_deleted_rows INTEGER NOT NULL DEFAULT 0,
			last_cleanup_duration_ms INTEGER NOT NULL DEFAULT 0,
			last_cleanup_error TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)
	`)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	dur := time.Since(started).Milliseconds()
	_, err := db.Exec(`
		INSERT INTO request_log_storage_state (
			id, last_cleanup_started_at, last_cleanup_finished_at, last_cleanup_status,
			last_cleanup_deleted_rows, last_cleanup_duration_ms, last_cleanup_error, updated_at
		) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			last_cleanup_started_at = excluded.last_cleanup_started_at,
			last_cleanup_finished_at = excluded.last_cleanup_finished_at,
			last_cleanup_status = excluded.last_cleanup_status,
			last_cleanup_deleted_rows = excluded.last_cleanup_deleted_rows,
			last_cleanup_duration_ms = excluded.last_cleanup_duration_ms,
			last_cleanup_error = excluded.last_cleanup_error,
			updated_at = excluded.updated_at
	`, started.UTC().Format(time.RFC3339Nano), now, status, deleted, dur, errMsg, now)
	if err != nil {
		log.Warnf("usage: record cleanup state: %v", err)
	}
}

// RequestLogStorageStatus is exposed to management UI.
type RequestLogStorageStatus struct {
	RetentionDays          int    `json:"retention_days"`
	ContentRetentionDays   int    `json:"content_retention_days"`
	CleanupEnabled         bool   `json:"cleanup_enabled"`
	CleanupIntervalMinutes int    `json:"cleanup_interval_minutes"`
	MaxRows                int    `json:"max_rows"`
	MaxMetadataSizeMB      int    `json:"max_metadata_size_mb"`
	MaxTotalSizeMB         int    `json:"max_total_size_mb"`
	MetadataRowCount       int64  `json:"metadata_row_count"`
	ContentRowCount        int64  `json:"content_row_count"`
	RollupRowCount         int64  `json:"rollup_row_count"`
	LastCleanupStatus      string `json:"last_cleanup_status"`
	LastCleanupDeletedRows int64  `json:"last_cleanup_deleted_rows"`
	LastCleanupDurationMs  int64  `json:"last_cleanup_duration_ms"`
	LastCleanupFinishedAt  string `json:"last_cleanup_finished_at,omitempty"`
	LastCleanupError       string `json:"last_cleanup_error,omitempty"`
	StatsNote              string `json:"stats_note"`
}

func GetRequestLogStorageStatus() (RequestLogStorageStatus, error) {
	cfg := currentRequestLogStorageConfig()
	st := RequestLogStorageStatus{
		RetentionDays:          cfg.RetentionDays,
		ContentRetentionDays:   cfg.ContentRetentionDays,
		CleanupEnabled:         cfg.CleanupEnabled,
		CleanupIntervalMinutes: cfg.CleanupIntervalMinutes,
		MaxRows:                cfg.MaxRows,
		MaxMetadataSizeMB:      cfg.MaxMetadataSizeMB,
		MaxTotalSizeMB:         cfg.MaxTotalSizeMB,
		StatsNote:              "Cleaning request_logs does not clear dashboard/end-user/apikey-lookup stats; stats come from usage_rollup_buckets.",
	}
	db := getReadDB()
	if db == nil {
		return st, nil
	}
	// Prefer maintained counters when present; fall back to approximate/cheap estimates.
	// Avoid three hot COUNT(*) on large tables in the common path.
	if err := db.QueryRow(`
		SELECT COALESCE(metadata_row_count, 0), COALESCE(content_row_count, 0)
		FROM request_log_storage_state WHERE id = 1
	`).Scan(&st.MetadataRowCount, &st.ContentRowCount); err != nil {
		// Best-effort fallbacks for fresh DBs.
		_ = db.QueryRow(`SELECT COUNT(*) FROM request_logs`).Scan(&st.MetadataRowCount)
		_ = db.QueryRow(`SELECT COUNT(*) FROM request_log_content`).Scan(&st.ContentRowCount)
	}
	// Rollup size is not on the hot path of proxy; approximate via marker table absence is fine.
	_ = db.QueryRow(`SELECT COUNT(*) FROM usage_rollup_buckets WHERE bucket_kind = 'lifetime'`).Scan(&st.RollupRowCount)
	_ = db.QueryRow(`
		SELECT COALESCE(last_cleanup_status,''), COALESCE(last_cleanup_deleted_rows,0),
		       COALESCE(last_cleanup_duration_ms,0), COALESCE(last_cleanup_finished_at,''),
		       COALESCE(last_cleanup_error,'')
		FROM request_log_storage_state WHERE id = 1
	`).Scan(&st.LastCleanupStatus, &st.LastCleanupDeletedRows, &st.LastCleanupDurationMs, &st.LastCleanupFinishedAt, &st.LastCleanupError)
	return st, nil
}
