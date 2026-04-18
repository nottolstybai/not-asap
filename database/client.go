package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client provides database operations for the notasap job queue.
// It holds a *pgxpool.Pool and offers enqueue, claim, and status update methods.
type Client struct {
	pool *pgxpool.Pool
}

// NewClient creates a Client with the provided connection pool.
func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// Pool returns the underlying *pgxpool.Pool for integration testing.
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// BatchJob represents a job to be inserted in a batch.
type BatchJob struct {
	Kind        string
	Args        any
	MaxAttempts int
}

// Enqueue inserts one job without an external transaction.
// max_attempts is provided by the caller (queue/ resolves config before calling).
func (c *Client) Enqueue(ctx context.Context, kind string, args any, maxAttempts int) (*Job, error) {
	rows, err := c.pool.Query(ctx, InsertJobSQL, kind, args, maxAttempts)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// EnqueueBatch inserts multiple jobs in a single pgx.Batch round-trip.
// Returns nil immediately if kindPayloads is empty.
func (c *Client) EnqueueBatch(ctx context.Context, kindPayloads []BatchJob) error {
	if len(kindPayloads) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, jp := range kindPayloads {
		batch.Queue(InsertJobSQL, jp.Kind, jp.Args, jp.MaxAttempts)
	}
	results := c.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range kindPayloads {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueTx inserts one job inside the caller's pgx.Tx.
func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, kind string, args any, maxAttempts int) (*Job, error) {
	rows, err := tx.Query(ctx, InsertJobSQL, kind, args, maxAttempts)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// EnqueueBatchTx inserts multiple jobs inside the caller's pgx.Tx using tx.SendBatch.
// Returns nil immediately if kindPayloads is empty.
func (c *Client) EnqueueBatchTx(ctx context.Context, tx pgx.Tx, kindPayloads []BatchJob) error {
	if len(kindPayloads) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, jp := range kindPayloads {
		batch.Queue(InsertJobSQL, jp.Kind, jp.Args, jp.MaxAttempts)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range kindPayloads {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

// ClaimJob atomically claims one job from the database.
// Returns nil, nil when no jobs are available (normal "queue empty" case).
func (c *Client) ClaimJob(ctx context.Context, workerID string) (*ClaimedJob, error) {
	rows, err := c.pool.Query(ctx, ClaimJobSQL, workerID)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[ClaimedJob])
	if err == pgx.ErrNoRows {
		return nil, nil // no jobs available
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// CompleteJob sets status to 'done'. Returns rowsAffected.
func (c *Client) CompleteJob(ctx context.Context, id, workerID string) (int64, error) {
	cmd, err := c.pool.Exec(ctx, CompleteJobSQL, string(StatusDone), id, workerID)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// FailJob sets status to 'failed' with the error message.
func (c *Client) FailJob(ctx context.Context, id, workerID, errMsg string) (int64, error) {
	cmd, err := c.pool.Exec(ctx, `
        UPDATE notasap_jobs SET status = $1, error = $2, updated_at = NOW()
        WHERE id = $3 AND worker_id = $4 AND status = 'running'`,
		string(StatusFailed), errMsg, id, workerID)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// CancelJob sets status to 'cancelled' immediately, regardless of remaining attempts.
func (c *Client) CancelJob(ctx context.Context, id, workerID string) (int64, error) {
	cmd, err := c.pool.Exec(ctx, `
        UPDATE notasap_jobs SET status = $1, updated_at = NOW()
        WHERE id = $2 AND worker_id = $3 AND status = 'running'`,
		string(StatusCancelled), id, workerID)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// ExhaustJob sets status to 'exhausted'.
func (c *Client) ExhaustJob(ctx context.Context, id, workerID string) (int64, error) {
	cmd, err := c.pool.Exec(ctx, `
        UPDATE notasap_jobs SET status = $1, updated_at = NOW()
        WHERE id = $2 AND worker_id = $3 AND status = 'running'`,
		string(StatusExhausted), id, workerID)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// RetryJob sets status to 'retrying' with the calculated delay.
func (c *Client) RetryJob(ctx context.Context, id, workerID, errMsg string, runAfter time.Time, newAttempt int) (int64, error) {
	cmd, err := c.pool.Exec(ctx, RetryJobSQL,
		string(StatusRetrying), runAfter, newAttempt, errMsg, id, workerID)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// HeartbeatBatch issues a single batched UPDATE for multiple job IDs.
func (c *Client) HeartbeatBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := c.pool.Exec(ctx, HeartbeatSQL, ids)
	return err
}

// JanitorDeleteBatch deletes terminal jobs older than ttl for the given status.
func (c *Client) JanitorDeleteBatch(ctx context.Context, status Status, ttl time.Duration) (int64, error) {
	cutoff := time.Now().Add(-ttl)
	result, err := c.pool.Exec(ctx, JanitorDeleteSQL, string(status), cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
