package pgtest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/internal/store"
)

const (
	// WaitTimeout is how long WaitFor and WaitStatus wait before failing.
	WaitTimeout = 10 * time.Second
	// Tick is the polling interval of WaitFor and Consistently.
	Tick = 10 * time.Millisecond
)

// jobColumns lists every notasap_jobs column in JobRow order.
const jobColumns = `id, kind, status, payload, attempt, max_attempts, run_at, deadline, lease,
       idempotency_key, error, created_at, updated_at`

// Fixture inserts and updates notasap_jobs rows directly, bypassing the store.
// Failures are reported on the T returned by t at call time, so a fixture
// shared by a suite fails the current test or subtest.
type Fixture struct {
	t    func() *testing.T
	pool *pgxpool.Pool
}

// NewFixture returns a fixture writing through pool; t is usually s.T.
func NewFixture(t func() *testing.T, pool *pgxpool.Pool) *Fixture {
	return &Fixture{
		t:    t,
		pool: pool,
	}
}

func opt[T any](opts []T) T {
	if len(opts) > 0 {
		return opts[0]
	}

	return lo.Empty[T]()
}

// JobOpt sets columns of a fixture row; nil means default (Job) or unchanged (SetJob).
// Durations are relative to the database NOW().
type JobOpt struct {
	Kind        *int16
	Status      *int16
	Payload     *string
	Attempt     *int
	MaxAttempts *int
	Lease       *int64
	RunIn       *time.Duration
	DeadlineIn  *time.Duration
	UpdatedAgo  *time.Duration
	Key         *string
	Error       *string
}

const insertJobSQL = `
INSERT INTO notasap_jobs (kind, status, payload, attempt, max_attempts, lease,
                          run_at, deadline, idempotency_key, error, updated_at)
SELECT $1::smallint, $2::smallint, $3::jsonb, $4::int, $5::int, $6::bigint,
       NOW() + make_interval(secs => $7::float8),
       NOW() + make_interval(secs => $8::float8), -- NULL stays NULL
       $9::text, $10::text,
       NOW() - make_interval(secs => $11::float8)
FROM generate_series(1, $12::int)
RETURNING ` + jobColumns

// Job inserts one row and returns it. Defaults: kind 1, pending, payload {},
// attempt 0, max_attempts 3, lease 0, due now, no deadline, updated now; a
// running row defaults attempt and lease to 1.
func (f *Fixture) Job(ctx context.Context, opts ...JobOpt) JobRow {
	t := f.t()
	t.Helper()
	rows := f.Jobs(ctx, 1, opts...)
	require.Len(t, rows, 1)

	return rows[0]
}

// Jobs inserts n identical rows in one statement; they share one NOW(), so
// run_at and updated_at tie exactly.
func (f *Fixture) Jobs(ctx context.Context, n int, opts ...JobOpt) []JobRow {
	t := f.t()
	t.Helper()
	o := opt(opts)

	status := lo.FromPtrOr(o.Status, store.StatusPending)
	attempt, lease := 0, int64(0)
	if status == store.StatusRunning {
		attempt, lease = 1, 1
	}

	var deadline *float64
	if o.DeadlineIn != nil {
		deadline = new(o.DeadlineIn.Seconds())
	}

	rows, err := f.pool.Query(ctx, insertJobSQL,
		lo.FromPtrOr(o.Kind, 1),
		status,
		lo.FromPtrOr(o.Payload, `{}`),
		lo.FromPtrOr(o.Attempt, attempt),
		lo.FromPtrOr(o.MaxAttempts, 3),
		lo.FromPtrOr(o.Lease, lease),
		lo.FromPtrOr(o.RunIn, 0).Seconds(),
		deadline,
		o.Key,
		o.Error,
		lo.FromPtrOr(o.UpdatedAgo, 0).Seconds(),
		n,
	)
	require.NoError(t, err)

	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[JobRow])
	require.NoError(t, err)

	return jobs
}

// SetJob updates only the non-nil columns of opt, e.g. to move a scheduled
// job into a given status for Cancel or idempotency tests.
func (f *Fixture) SetJob(ctx context.Context, id int64, opt JobOpt) {
	t := f.t()
	t.Helper()

	var sets []string
	var args []any
	add := func(col string, expr string, v any) {
		args = append(args, v)
		sets = append(sets, col+" = "+fmt.Sprintf(expr, len(args)))
	}
	if opt.Kind != nil {
		add("kind", "$%d", *opt.Kind)
	}
	if opt.Status != nil {
		add("status", "$%d", *opt.Status)
	}
	if opt.Payload != nil {
		add("payload", "$%d::jsonb", *opt.Payload)
	}
	if opt.Attempt != nil {
		add("attempt", "$%d", *opt.Attempt)
	}
	if opt.MaxAttempts != nil {
		add("max_attempts", "$%d", *opt.MaxAttempts)
	}
	if opt.Lease != nil {
		add("lease", "$%d", *opt.Lease)
	}
	if opt.RunIn != nil {
		add("run_at", "NOW() + make_interval(secs => $%d::float8)", opt.RunIn.Seconds())
	}
	if opt.DeadlineIn != nil {
		add("deadline", "NOW() + make_interval(secs => $%d::float8)", opt.DeadlineIn.Seconds())
	}
	if opt.UpdatedAgo != nil {
		add("updated_at", "NOW() - make_interval(secs => $%d::float8)", opt.UpdatedAgo.Seconds())
	}
	if opt.Key != nil {
		add("idempotency_key", "$%d", *opt.Key)
	}
	if opt.Error != nil {
		add("error", "$%d", *opt.Error)
	}
	if len(sets) == 0 {
		return
	}

	args = append(args, id)
	tag, err := f.pool.Exec(ctx,
		fmt.Sprintf("UPDATE notasap_jobs SET %s WHERE id = $%d", strings.Join(sets, ", "), len(args)), args...)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "SetJob: no job %d", id)
}

// Exists reports whether job id is in the table.
func (s *Suite) Exists(id int64) bool {
	s.T().Helper()
	var ok bool
	s.Require().NoError(s.Pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM notasap_jobs WHERE id = $1)`, id).Scan(&ok))
	return ok
}

// CountJobs counts rows in the given statuses; with none, all rows.
func (s *Suite) CountJobs(statuses ...int16) int {
	s.T().Helper()
	q, args := `SELECT count(*) FROM notasap_jobs`, []any(nil)
	if len(statuses) > 0 {
		q += ` WHERE status = ANY($1)`
		args = append(args, statuses)
	}
	var n int
	s.Require().NoError(s.Pool.QueryRow(context.Background(), q, args...).Scan(&n))
	return n
}

// DBNow returns the database clock.
func (s *Suite) DBNow() time.Time {
	s.T().Helper()
	var t time.Time
	s.Require().NoError(s.Pool.QueryRow(context.Background(), `SELECT NOW()`).Scan(&t))
	return t
}

// WaitFor polls cond on the test goroutine until it returns true or
// WaitTimeout elapses. cond may use s.Require(): it runs on the test goroutine.
func (s *Suite) WaitFor(cond func() bool, msg string, args ...any) {
	s.T().Helper()
	deadline := time.Now().Add(WaitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			s.FailNowf("timed out", msg, args...)
		}
		time.Sleep(Tick)
	}
}

// BackendPID returns the backend pid of tx.
func (s *Suite) BackendPID(tx pgx.Tx) uint32 {
	s.T().Helper()
	var pid uint32
	s.Require().NoError(tx.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

// WaitBlockedBy waits until some backend waits on a lock held by pid.
func (s *Suite) WaitBlockedBy(pid uint32) {
	s.T().Helper()
	s.WaitFor(func() bool {
		var n int
		s.Require().NoError(s.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", pid).Scan(&n))
		return n > 0
	}, "no backend blocked by pid %d", pid)
}

// Consistently fails if cond becomes true within d. Use only when no barrier
// exists to wait on.
func (s *Suite) Consistently(d time.Duration, cond func() bool, msg string, args ...any) {
	s.T().Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			s.FailNowf("condition became true", msg, args...)
		}
		time.Sleep(Tick)
	}
}

// WaitStatus waits until job id has status st and returns the row.
func WaitStatus[S ~int16](s *Suite, id int64, st S) JobRow {
	s.T().Helper()
	var row JobRow
	s.WaitFor(func() bool {
		row = s.GetJob(id)
		return row.Status == int16(st)
	}, "job %d did not reach status %d", id, int16(st))
	return row
}
