package pgtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

const (
	// WaitTimeout is how long WaitFor and WaitStatus wait before failing.
	WaitTimeout = 10 * time.Second
	// Tick is the polling interval of WaitFor and Consistently.
	Tick = 10 * time.Millisecond
)

// Bits of jobSpec.set, one per column that SetJob can update.
const (
	fKind uint16 = 1 << iota
	fStatus
	fPayload
	fAttempt
	fLease
	fRunAt
	fDeadline
	fUpdatedAt
	fKey
	fError
)

// JobOption sets one column of a fixture row. Times are relative to the
// database NOW(), never to the Go clock.
type JobOption func(*jobSpec)

type jobSpec struct {
	kind        int16
	status      int16
	payload     string
	attempt     int
	maxAttempts int
	lease       int64
	runIn       time.Duration  // run_at = NOW() + runIn
	deadlineIn  *time.Duration // deadline = NOW() + deadlineIn; nil = NULL
	updatedAgo  time.Duration  // updated_at = NOW() - updatedAgo
	key, errMsg *string
	set         uint16 // bitmask of explicitly set fields, for SetJob
}

func defaults() jobSpec {
	return jobSpec{kind: 1, status: store.StatusPending, payload: `{}`, maxAttempts: 3}
}

// WithKind sets the kind column.
func WithKind[K ~int16](k K) JobOption {
	return func(j *jobSpec) { j.kind = int16(k); j.set |= fKind }
}

// WithStatus sets the status column.
func WithStatus[S ~int16](s S) JobOption {
	return func(j *jobSpec) { j.status = int16(s); j.set |= fStatus }
}

// WithPayload sets the payload column to the JSON text.
func WithPayload(json string) JobOption {
	return func(j *jobSpec) { j.payload = json; j.set |= fPayload }
}

// WithAttempt sets the attempt and max_attempts columns.
func WithAttempt(attempt, max int) JobOption {
	return func(j *jobSpec) { j.attempt, j.maxAttempts = attempt, max; j.set |= fAttempt }
}

// WithLease sets the lease column.
func WithLease(n int64) JobOption {
	return func(j *jobSpec) { j.lease = n; j.set |= fLease }
}

// WithRunIn sets run_at to NOW() + d; negative means due.
func WithRunIn(d time.Duration) JobOption {
	return func(j *jobSpec) { j.runIn = d; j.set |= fRunAt }
}

// WithDeadlineIn sets deadline to NOW() + d; negative means expired.
func WithDeadlineIn(d time.Duration) JobOption {
	return func(j *jobSpec) { j.deadlineIn = &d; j.set |= fDeadline }
}

// WithUpdatedAgo sets updated_at to NOW() - d.
func WithUpdatedAgo(d time.Duration) JobOption {
	return func(j *jobSpec) { j.updatedAgo = d; j.set |= fUpdatedAt }
}

// WithKey sets the idempotency_key column.
func WithKey(k string) JobOption {
	return func(j *jobSpec) { j.key = &k; j.set |= fKey }
}

// WithError sets the error column.
func WithError(msg string) JobOption {
	return func(j *jobSpec) { j.errMsg = &msg; j.set |= fError }
}

// Running makes a claimed job: status running and a deadline deadlineIn from
// now. It names no other column, so SetJob leaves them alone; InsertJob
// defaults attempt and lease to 1 for a running row.
func Running(deadlineIn time.Duration) JobOption {
	return func(j *jobSpec) {
		WithStatus(store.StatusRunning)(j)
		WithDeadlineIn(deadlineIn)(j)
	}
}

// Finished makes a job in a final status last updated ago in the past.
func Finished[S ~int16](status S, ago time.Duration) JobOption {
	return func(j *jobSpec) {
		WithStatus(status)(j)
		WithUpdatedAgo(ago)(j)
	}
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
RETURNING id`

// InsertJob inserts one row and returns its id.
func (s *Suite) InsertJob(opts ...JobOption) int64 {
	s.T().Helper()
	return s.InsertJobs(1, opts...)[0]
}

// InsertJobs inserts n identical rows in one statement; they share one NOW(),
// so run_at and updated_at tie exactly.
func (s *Suite) InsertJobs(n int, opts ...JobOption) []int64 {
	s.T().Helper()
	j := defaults()
	for _, o := range opts {
		o(&j)
	}
	if j.status == store.StatusRunning {
		if j.set&fAttempt == 0 {
			j.attempt = 1
		}
		if j.set&fLease == 0 {
			j.lease = 1
		}
	}
	var deadline *float64
	if j.deadlineIn != nil {
		d := j.deadlineIn.Seconds()
		deadline = &d
	}
	rows, err := s.Pool.Query(context.Background(), insertJobSQL,
		j.kind, j.status, j.payload, j.attempt, j.maxAttempts, j.lease,
		j.runIn.Seconds(), deadline, j.key, j.errMsg, j.updatedAgo.Seconds(), n)
	s.Require().NoError(err)
	defer rows.Close()
	ids := make([]int64, 0, n)
	for rows.Next() {
		var id int64
		s.Require().NoError(rows.Scan(&id))
		ids = append(ids, id)
	}
	s.Require().NoError(rows.Err())
	return ids
}

// SetJob updates only the columns set by opts, e.g. to move a scheduled job
// into a given status for Cancel or idempotency tests.
func (s *Suite) SetJob(id int64, opts ...JobOption) {
	s.T().Helper()
	j := defaults()
	for _, o := range opts {
		o(&j)
	}
	var sets []string
	var args []any
	add := func(col string, expr string, v any) {
		args = append(args, v)
		sets = append(sets, col+" = "+fmt.Sprintf(expr, len(args)))
	}
	if j.set&fKind != 0 {
		add("kind", "$%d", j.kind)
	}
	if j.set&fStatus != 0 {
		add("status", "$%d", j.status)
	}
	if j.set&fPayload != 0 {
		add("payload", "$%d::jsonb", j.payload)
	}
	if j.set&fAttempt != 0 {
		add("attempt", "$%d", j.attempt)
		add("max_attempts", "$%d", j.maxAttempts)
	}
	if j.set&fLease != 0 {
		add("lease", "$%d", j.lease)
	}
	if j.set&fRunAt != 0 {
		add("run_at", "NOW() + make_interval(secs => $%d::float8)", j.runIn.Seconds())
	}
	if j.set&fDeadline != 0 {
		add("deadline", "NOW() + make_interval(secs => $%d::float8)", j.deadlineIn.Seconds())
	}
	if j.set&fUpdatedAt != 0 {
		add("updated_at", "NOW() - make_interval(secs => $%d::float8)", j.updatedAgo.Seconds())
	}
	if j.set&fKey != 0 {
		add("idempotency_key", "$%d", j.key)
	}
	if j.set&fError != 0 {
		add("error", "$%d", j.errMsg)
	}
	if len(sets) == 0 {
		return
	}
	args = append(args, id)
	tag, err := s.Pool.Exec(context.Background(),
		fmt.Sprintf("UPDATE notasap_jobs SET %s WHERE id = $%d", strings.Join(sets, ", "), len(args)), args...)
	s.Require().NoError(err)
	s.Require().EqualValues(1, tag.RowsAffected(), "SetJob: no job %d", id)
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
		row = s.Job(id)
		return row.Status == int16(st)
	}, "job %d did not reach status %d", id, int16(st))
	return row
}
