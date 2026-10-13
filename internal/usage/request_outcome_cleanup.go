package usage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Outcome metadata follows request-record retention, never body/detail
// retention. Batches and the shared cleanup deadline bound maintenance work.
func cleanupExpiredRequestOutcomes(ctx context.Context, db *sql.DB, cfg requestLogStorageRuntime, batch int, deadline time.Time) error {
	for ctx.Err() == nil && time.Now().Before(deadline) {
		var deleted int64
		if cfg.RetentionDays > 0 {
			cutoff := time.Now().UTC().AddDate(0, 0, -cfg.RetentionDays).Format(time.RFC3339Nano)
			result, err := db.ExecContext(ctx, `DELETE FROM request_outcomes WHERE (tenant_id, request_id) IN
			(SELECT tenant_id, request_id FROM request_outcomes WHERE timestamp < ? ORDER BY timestamp ASC LIMIT ?)`, cutoff, batch)
			if err != nil {
				return fmt.Errorf("usage: expire request outcomes: %w", err)
			}
			deleted, _ = result.RowsAffected()
		}
		if deleted == 0 && cfg.MaxRows > 0 {
			var count int64
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM request_outcomes").Scan(&count); err != nil {
				return err
			}
			if count > int64(cfg.MaxRows) {
				limit := min(int64(batch), count-int64(cfg.MaxRows))
				result, err := db.ExecContext(ctx, `DELETE FROM request_outcomes WHERE (tenant_id, request_id) IN
				(SELECT tenant_id, request_id FROM request_outcomes ORDER BY timestamp ASC LIMIT ?)`, limit)
				if err != nil {
					return fmt.Errorf("usage: cap request outcomes: %w", err)
				}
				deleted, _ = result.RowsAffected()
			}
		}
		if deleted == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return ctx.Err()
}
