package store

import (
	"context"
	"fmt"
	"time"
)

const deleteFinishedSQL = `DELETE FROM notasap_jobs WHERE id IN (
    SELECT id FROM notasap_jobs
    WHERE status = $1 AND updated_at < NOW() - make_interval(secs => $2)
    ORDER BY updated_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED)`

// DeleteFinished deletes up to limit jobs in status that were last updated
// more than olderThan ago, oldest first, and returns how many it deleted.
// Rows locked by other transactions are skipped.
func (s *Store) DeleteFinished(ctx context.Context, status int16, olderThan time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, deleteFinishedSQL, status, olderThan.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("notasap: delete finished: %w", err)
	}
	return tag.RowsAffected(), nil
}
