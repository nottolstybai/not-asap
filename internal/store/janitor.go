package store

import (
	"context"
	"fmt"
	"time"
)

// The pg_try_advisory_xact_lock call sits in an uncorrelated scalar subquery,
// so Postgres runs it once as an InitPlan, not per row. A replica that loses
// the lock scans nothing. The lock is released when the statement's implicit
// transaction ends. The redundant "status IN (3,4,5)" lets a generic plan
// (status = $1) prove the partial janitor index predicate.
const deleteFinishedSQL = `DELETE FROM notasap_jobs WHERE id IN (
    SELECT id FROM notasap_jobs
    WHERE (SELECT pg_try_advisory_xact_lock('notasap_jobs'::regclass::oid::int, 1))
      AND status IN (3,4,5) AND status = $1
      AND updated_at < NOW() - make_interval(secs => $2)
    ORDER BY updated_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED)`

// DeleteFinished deletes up to limit jobs in status that were last updated
// more than olderThan ago, oldest first, and returns how many it deleted.
// Rows locked by other transactions are skipped. Only one replica deletes at a
// time: if another one holds the janitor advisory lock, it returns 0.
func (s *Store) DeleteFinished(ctx context.Context, status int16, olderThan time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, deleteFinishedSQL, status, olderThan.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("notasap: delete finished: %w", err)
	}
	return tag.RowsAffected(), nil
}
