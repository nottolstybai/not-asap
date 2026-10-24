package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// maxInsertRounds bounds the insert/lookup loop that handles rows deleted
// (e.g. by the janitor) between a key conflict and its lookup.
const maxInsertRounds = 3

const insertSQL = `INSERT INTO notasap_jobs (kind, status, payload, max_attempts, run_at, idempotency_key)
VALUES ($1, 1, $2, $3, NOW() + make_interval(secs => $4), $5)
ON CONFLICT (kind, idempotency_key) WHERE idempotency_key IS NOT NULL `

const insertNothingSQL = insertSQL + `DO NOTHING
RETURNING id, (xmax = 0) AS inserted`

const insertReplaceSQL = insertSQL + `DO UPDATE SET payload = EXCLUDED.payload, status = 1, attempt = 0,
    max_attempts = EXCLUDED.max_attempts, run_at = EXCLUDED.run_at, error = NULL, deadline = NULL,
    updated_at = NOW()
WHERE notasap_jobs.status <> 2
RETURNING id, (xmax = 0) AS inserted`

const lookupSQL = `SELECT id, status FROM notasap_jobs WHERE kind = $1 AND idempotency_key = $2`

// InsertParams describes one job to insert.
type InsertParams struct {
	Kind        int16
	Payload     []byte
	MaxAttempts int
	Delay       time.Duration
	Key         *string
	Replace     bool
}

// InsertOutcome says what Insert did with one job.
type InsertOutcome int

// Insert outcomes.
const (
	// Inserted means a new row was created.
	Inserted InsertOutcome = iota + 1
	// Replaced means an existing row with the same key was overwritten.
	Replaced
	// Duplicate means a row with the same key exists and was left as is.
	Duplicate
	// Running means Replace was requested but the existing row is running.
	Running
)

// InsertResult is the outcome of one InsertParams; ID is the new or existing row.
type InsertResult struct {
	ID      int64
	Outcome InsertOutcome
}

// Insert inserts params in one batch and resolves idempotency key conflicts.
// Results are in the order of params. Each round trip is a single batch, so
// outside a transaction a round is atomic, but rounds after the first (key
// conflict lookups and re-inserts) commit separately: callers needing the
// whole call to be atomic must pass a transaction.
func (s *Store) Insert(ctx context.Context, db DB, params []InsertParams) ([]InsertResult, error) {
	results := make([]InsertResult, len(params))
	todo := make([]int, len(params))
	for i := range params {
		todo[i] = i
	}
	for range maxInsertRounds {
		conflicts, err := insertRound(ctx, db, params, todo, results)
		if err != nil {
			return nil, err
		}
		if len(conflicts) == 0 {
			return results, nil
		}
		todo, err = lookupRound(ctx, db, params, conflicts, results)
		if err != nil {
			return nil, err
		}
		if len(todo) == 0 {
			return results, nil
		}
	}
	return nil, fmt.Errorf("notasap: insert: idempotency key conflict unresolved after %d rounds", maxInsertRounds)
}

// insertRound inserts params[todo] and returns the indexes that hit a key
// conflict without returning a row.
func insertRound(ctx context.Context, db DB, params []InsertParams, todo []int, results []InsertResult) ([]int, error) {
	b := &pgx.Batch{}
	for _, i := range todo {
		p := params[i]
		sql := insertNothingSQL
		if p.Replace {
			sql = insertReplaceSQL
		}
		b.Queue(sql, p.Kind, p.Payload, p.MaxAttempts, p.Delay.Seconds(), p.Key)
	}
	br := db.SendBatch(ctx, b)
	var conflicts []int
	for _, i := range todo {
		var (
			id       int64
			inserted bool
		)
		err := br.QueryRow().Scan(&id, &inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			if params[i].Key == nil {
				_ = br.Close()
				return nil, errors.New("notasap: insert: no row returned for a job without idempotency key")
			}
			conflicts = append(conflicts, i)
			continue
		}
		if err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("notasap: insert: %w", err)
		}
		results[i] = InsertResult{ID: id, Outcome: Replaced}
		if inserted {
			results[i].Outcome = Inserted
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("notasap: insert: %w", err)
	}
	return conflicts, nil
}

// lookupRound reads the existing rows for conflicting params and returns the
// indexes that must be inserted again: rows that were deleted meanwhile, and
// replace requests whose row stopped running meanwhile.
func lookupRound(ctx context.Context, db DB, params []InsertParams, conflicts []int, results []InsertResult) ([]int, error) {
	b := &pgx.Batch{}
	for _, i := range conflicts {
		b.Queue(lookupSQL, params[i].Kind, params[i].Key)
	}
	br := db.SendBatch(ctx, b)
	var retry []int
	for _, i := range conflicts {
		var (
			id     int64
			status int16
		)
		err := br.QueryRow().Scan(&id, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			retry = append(retry, i)
			continue
		}
		if err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("notasap: insert: lookup: %w", err)
		}
		switch {
		case !params[i].Replace:
			results[i] = InsertResult{ID: id, Outcome: Duplicate}
		case status == statusRunning:
			results[i] = InsertResult{ID: id, Outcome: Running}
		default:
			retry = append(retry, i)
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("notasap: insert: lookup: %w", err)
	}
	return retry, nil
}
