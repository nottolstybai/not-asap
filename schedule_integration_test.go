package notasap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type ScheduleSuite struct {
	pgtest.Suite
}

func TestScheduleSuite(t *testing.T) {
	suite.Run(t, new(ScheduleSuite))
}

func (s *ScheduleSuite) newClient(opts ...notasap.Option) *notasap.Client {
	c, err := notasap.New(s.Pool, opts...)
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c, noop[payloadA], notasap.KindConfig{Name: "a", MaxAttempts: 7}))
	s.Require().NoError(notasap.Register(c, noop[payloadB], notasap.KindConfig{Name: "b"}))
	return c
}

func (s *ScheduleSuite) dbNow() time.Time {
	var now time.Time
	s.Require().NoError(s.Pool.QueryRow(context.Background(), "SELECT now()").Scan(&now))
	return now
}

func (s *ScheduleSuite) TestScheduleInsertsRow() {
	c := s.newClient()
	p := payloadA{Msg: "hello", N: 3}
	id, err := c.Schedule(context.Background(), p)
	s.Require().NoError(err)

	row := s.Job(id)
	s.Equal(int16(kindA), row.Kind)
	s.Equal(int16(notasap.StatusPending), row.Status)
	s.Equal(0, row.Attempt)
	s.Equal(7, row.MaxAttempts)
	s.Nil(row.Key)
	want, err := json.Marshal(p)
	s.Require().NoError(err)
	s.JSONEq(string(want), string(row.Payload))
	s.WithinDuration(s.dbNow(), row.RunAt, time.Second)
}

func (s *ScheduleSuite) TestScheduleDelay() {
	c := s.newClient()
	id, err := c.Schedule(context.Background(), payloadA{}, notasap.WithDelay(time.Hour))
	s.Require().NoError(err)

	row := s.Job(id)
	d := row.RunAt.Sub(row.CreatedAt)
	s.GreaterOrEqual(d, time.Hour-time.Second)
	s.LessOrEqual(d, time.Hour+time.Second)
}

func (s *ScheduleSuite) TestUnknownKind() {
	c := s.newClient()
	_, err := c.Schedule(context.Background(), unregisteredPayload{})
	s.Require().ErrorIs(err, notasap.ErrUnknownKind)
	s.Equal(0, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestScheduleTxCommitRollback() {
	ctx := context.Background()
	c := s.newClient()

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	id, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "commit"})
	s.Require().NoError(err)
	s.Require().NoError(tx.Commit(ctx))
	s.Equal(int16(notasap.StatusPending), s.Job(id).Status)

	tx, err = s.Pool.Begin(ctx)
	s.Require().NoError(err)
	_, err = c.ScheduleTx(ctx, tx, payloadA{Msg: "rollback"})
	s.Require().NoError(err)
	s.Require().NoError(tx.Rollback(ctx))
	s.Equal(1, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestIdempotency() {
	ctx := context.Background()
	c := s.newClient()
	statuses := []notasap.Status{
		notasap.StatusPending, notasap.StatusDone, notasap.StatusFailed,
		notasap.StatusCancelled, notasap.StatusRunning,
	}
	for _, st := range statuses {
		for _, replace := range []bool{false, true} {
			s.Run(fmt.Sprintf("%s/replace=%v", st, replace), func() {
				key := fmt.Sprintf("key-%s-%v", st, replace)
				origID, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey(key))
				s.Require().NoError(err)
				s.Exec(`UPDATE notasap_jobs SET status = $2, attempt = 2, max_attempts = 99,
					error = 'boom', deadline = NOW() WHERE id = $1`, origID, int16(st))

				opts := []notasap.ScheduleOption{notasap.WithIdempotencyKey(key)}
				if replace {
					opts = append(opts, notasap.WithReplace())
				}
				id, err := c.Schedule(ctx, payloadA{Msg: "new"}, opts...)
				s.Equal(origID, id)

				row := s.Job(origID)
				switch {
				case !replace:
					s.Require().ErrorIs(err, notasap.ErrDuplicate)
					s.Equal(int16(st), row.Status)
					s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
				case st == notasap.StatusRunning:
					s.Require().ErrorIs(err, notasap.ErrJobRunning)
					s.Equal(int16(st), row.Status)
					s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
				default:
					s.Require().NoError(err)
					s.Equal(int16(notasap.StatusPending), row.Status)
					s.Equal(0, row.Attempt)
					s.Equal(7, row.MaxAttempts)
					s.Nil(row.Error)
					s.Nil(row.Deadline)
					s.JSONEq(`{"msg":"new","n":0}`, string(row.Payload))
				}
			})
		}
	}
	s.Equal(len(statuses)*2, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestIdempotencySameKeyDifferentKind() {
	ctx := context.Background()
	c := s.newClient()
	idA, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
	s.Require().NoError(err)
	idB, err := c.Schedule(ctx, payloadB{}, notasap.WithIdempotencyKey("k"))
	s.Require().NoError(err)
	s.NotEqual(idA, idB)
	s.Equal(2, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestIdempotencyConcurrent() {
	ctx := context.Background()
	c := s.newClient()
	const n = 20
	ids := make([]int64, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			ids[i], errs[i] = c.Schedule(ctx, payloadA{N: i}, notasap.WithIdempotencyKey("same"))
		})
	}
	close(start)
	wg.Wait()

	var ok, dup int
	for i := range n {
		switch {
		case errs[i] == nil:
			ok++
		case s.ErrorIs(errs[i], notasap.ErrDuplicate):
			dup++
		}
		s.Equal(ids[0], ids[i])
	}
	s.Equal(1, ok)
	s.Equal(n-1, dup)
	s.Equal(1, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestReplaceRolledBack() {
	ctx := context.Background()
	c := s.newClient()
	id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey("k"))
	s.Require().NoError(err)
	s.Exec("UPDATE notasap_jobs SET status = 4, error = 'boom' WHERE id = $1", id)

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	got, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
	s.Require().NoError(err)
	s.Equal(id, got)
	s.Require().NoError(tx.Rollback(ctx))

	row := s.Job(id)
	s.Equal(int16(notasap.StatusFailed), row.Status)
	s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
}

func (s *ScheduleSuite) TestBatch() {
	ctx := context.Background()
	c := s.newClient()
	dupID, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
	s.Require().NoError(err)
	runID, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("run"))
	s.Require().NoError(err)
	s.Exec("UPDATE notasap_jobs SET status = 2 WHERE id = $1", runID)

	res, err := c.ScheduleBatch(ctx,
		notasap.Item(payloadA{Msg: "new"}),
		notasap.Item(payloadA{}, notasap.WithIdempotencyKey("dup")),
		notasap.Item(payloadA{}, notasap.WithIdempotencyKey("run"), notasap.WithReplace()),
	)
	s.Require().NoError(err)
	s.Require().Len(res, 3)
	s.NoError(res[0].Err)
	s.NotZero(res[0].ID)
	s.NotEqual(dupID, res[0].ID)
	s.NotEqual(runID, res[0].ID)
	s.Equal(dupID, res[1].ID)
	s.ErrorIs(res[1].Err, notasap.ErrDuplicate)
	s.Equal(runID, res[2].ID)
	s.ErrorIs(res[2].Err, notasap.ErrJobRunning)
	s.Equal(3, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestBatchAtomicOnError() {
	c := s.newClient()
	_, err := c.ScheduleBatch(context.Background(),
		notasap.Item(payloadA{Msg: "one"}),
		notasap.Item(payloadA{Msg: "\x00"}),
		notasap.Item(payloadA{Msg: "three"}),
	)
	s.Require().ErrorContains(err, "unsupported Unicode escape")
	s.Equal(0, countJobs(&s.Suite))
}

// failingBatchDB sends the batch number failAt (1-based, counted over the pool
// and the transactions it begins) with a cancelled context, so it fails.
type failingBatchDB struct {
	*pgxpool.Pool
	failAt int32
	calls  *atomic.Int32
}

func (d failingBatchDB) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return d.Pool.SendBatch(failCtx(ctx, d.calls.Add(1) == d.failAt), b)
}

func (d failingBatchDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return failingBatchTx{Tx: tx, failAt: d.failAt, calls: d.calls}, nil
}

type failingBatchTx struct {
	pgx.Tx
	failAt int32
	calls  *atomic.Int32
}

func (t failingBatchTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.Tx.SendBatch(failCtx(ctx, t.calls.Add(1) == t.failAt), b)
}

func failCtx(ctx context.Context, fail bool) context.Context {
	if !fail {
		return ctx
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	return ctx
}

func (s *ScheduleSuite) TestBatchAtomicOnLookupError() {
	ctx := context.Background()
	// The key is committed while the batch waits on it, after the batch
	// statement's snapshot, so the batch falls back to the lookup round.
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = s.newClient().ScheduleTx(ctx, tx, payloadA{}, notasap.WithIdempotencyKey("dup"))
	s.Require().NoError(err)

	db := failingBatchDB{Pool: s.Pool, failAt: 2, calls: new(atomic.Int32)}
	c, err := notasap.NewWithDB(s.Pool, db)
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c, noop[payloadA], notasap.KindConfig{Name: "a"}))

	var (
		batchErr error
		wg       sync.WaitGroup
	)
	wg.Go(func() {
		_, batchErr = c.ScheduleBatch(ctx,
			notasap.Item(payloadA{Msg: "new"}),
			notasap.Item(payloadA{}, notasap.WithIdempotencyKey("dup")),
		)
	})
	s.waitBlockedBy(s.backendPID(tx))
	s.Require().NoError(tx.Commit(ctx))
	wg.Wait()
	s.Require().ErrorIs(batchErr, context.Canceled)
	s.Equal(int32(2), db.calls.Load())
	s.Equal(1, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestBatchTxRollback() {
	ctx := context.Background()
	c := s.newClient()
	items := []notasap.BatchItem{
		notasap.Item(payloadA{N: 1}),
		notasap.Item(payloadA{N: 2}),
		notasap.Item(payloadB{}),
	}

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	_, err = c.ScheduleBatchTx(ctx, tx, items...)
	s.Require().NoError(err)
	s.Require().NoError(tx.Rollback(ctx))
	s.Equal(0, countJobs(&s.Suite))

	res, err := c.ScheduleBatch(ctx, items...)
	s.Require().NoError(err)
	s.Require().Len(res, 3)
	for _, r := range res {
		s.NoError(r.Err)
	}
	s.Equal(3, countJobs(&s.Suite))
}

func (s *ScheduleSuite) TestScheduledObserver() {
	ctx := context.Background()
	obs := newCountingObserver()
	c := s.newClient(notasap.WithObserver(obs))

	_, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
	s.Require().NoError(err)
	_, err = c.Schedule(ctx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
	s.Require().NoError(err)
	_, err = c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
	s.Require().ErrorIs(err, notasap.ErrDuplicate)

	s.Equal(2, obs.Scheduled("a"))
	s.Equal(0, obs.Scheduled("b"))
}

// backendPID returns the backend pid of tx.
func (s *ScheduleSuite) backendPID(tx pgx.Tx) uint32 {
	s.T().Helper()
	var pid uint32
	s.Require().NoError(tx.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&pid))
	return pid
}

// waitBlockedBy waits until some backend waits on a lock held by pid.
func (s *ScheduleSuite) waitBlockedBy(pid uint32) {
	s.T().Helper()
	s.Eventually(func() bool {
		var n int
		err := s.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", pid).Scan(&n)
		return err == nil && n > 0
	}, 5*time.Second, 5*time.Millisecond)
}

func (s *ScheduleSuite) TestBatchOppositeKeyOrderNoDeadlock() {
	ctx := context.Background()
	c := s.newClient()
	for _, replace := range []bool{false, true} {
		s.Run(fmt.Sprintf("replace=%v", replace), func() {
			k1, k2 := fmt.Sprintf("k1-%v", replace), fmt.Sprintf("k2-%v", replace)
			opts := func(key string) []notasap.ScheduleOption {
				o := []notasap.ScheduleOption{notasap.WithIdempotencyKey(key)}
				if replace {
					o = append(o, notasap.WithReplace())
				}
				return o
			}
			if replace {
				for _, k := range []string{k1, k2} {
					id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey(k))
					s.Require().NoError(err)
					s.Exec("UPDATE notasap_jobs SET status = 3 WHERE id = $1", id)
				}
			}

			txA, err := s.Pool.Begin(ctx)
			s.Require().NoError(err)
			defer func() { _ = txA.Rollback(ctx) }()
			txB, err := s.Pool.Begin(ctx)
			s.Require().NoError(err)
			defer func() { _ = txB.Rollback(ctx) }()

			idA1, err := c.ScheduleTx(ctx, txA, payloadA{Msg: "A"}, opts(k1)...)
			s.Require().NoError(err)

			var (
				resB []notasap.BatchResult
				errB error
				wg   sync.WaitGroup
			)
			wg.Go(func() {
				resB, errB = c.ScheduleBatchTx(ctx, txB,
					notasap.Item(payloadA{Msg: "B"}, opts(k2)...),
					notasap.Item(payloadA{Msg: "B"}, opts(k1)...),
				)
			})
			s.waitBlockedBy(s.backendPID(txA))

			idA2, errA := c.ScheduleTx(ctx, txA, payloadA{Msg: "A"}, opts(k2)...)
			if errA == nil {
				errA = txA.Commit(ctx)
			}
			wg.Wait()
			if errB == nil {
				errB = txB.Commit(ctx)
			}
			for _, err := range []error{errA, errB} {
				if err != nil {
					s.NotContains(err.Error(), "40P01")
					s.NotContains(err.Error(), "deadlock")
				}
			}
			s.Require().NoError(errA)
			s.Require().NoError(errB)
			s.Require().Len(resB, 2)
			s.Equal(idA2, resB[0].ID)
			s.Equal(idA1, resB[1].ID)
			for _, r := range resB {
				if replace {
					s.NoError(r.Err)
				} else {
					s.ErrorIs(r.Err, notasap.ErrDuplicate)
				}
			}
			for _, k := range []string{k1, k2} {
				var n int
				s.Require().NoError(s.Pool.QueryRow(ctx,
					"SELECT count(*) FROM notasap_jobs WHERE idempotency_key = $1", k).Scan(&n))
				s.Equal(1, n)
			}
		})
	}
}

func (s *ScheduleSuite) TestReplaceRunningTakesNoLock() {
	ctx := context.Background()
	c := s.newClient()
	id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey("k"))
	s.Require().NoError(err)
	s.Exec("UPDATE notasap_jobs SET status = 2, lease = 1 WHERE id = $1", id)

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(ctx) }()
	got, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
	s.Require().ErrorIs(err, notasap.ErrJobRunning)
	s.Equal(id, got)

	finishCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	tag, err := s.Pool.Exec(finishCtx,
		"UPDATE notasap_jobs SET status = 3 WHERE id = $1 AND lease = 1 AND status = 2", id)
	s.Require().NoError(err)
	s.Equal(int64(1), tag.RowsAffected())
	s.Require().NoError(tx.Rollback(ctx))
}

// countingDB counts round trips sent through the pool.
type countingDB struct {
	*pgxpool.Pool
	calls *atomic.Int32
}

func (d countingDB) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	d.calls.Add(1)
	return d.Pool.SendBatch(ctx, b)
}

func (d countingDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.calls.Add(1)
	return d.Pool.Query(ctx, sql, args...)
}

func (d countingDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	d.calls.Add(1)
	return d.Pool.QueryRow(ctx, sql, args...)
}

func (d countingDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.calls.Add(1)
	return d.Pool.Exec(ctx, sql, args...)
}

func (s *ScheduleSuite) TestDuplicateSingleRoundTrip() {
	ctx := context.Background()
	id, err := s.newClient().Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
	s.Require().NoError(err)

	db := countingDB{Pool: s.Pool, calls: new(atomic.Int32)}
	c, err := notasap.NewWithDB(s.Pool, db)
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c, noop[payloadA], notasap.KindConfig{Name: "a"}))

	got, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
	s.Require().ErrorIs(err, notasap.ErrDuplicate)
	s.Equal(id, got)
	s.Equal(int32(1), db.calls.Load())
}

func (s *ScheduleSuite) TestSameKeyTwiceInBatch() {
	ctx := context.Background()
	c := s.newClient()

	res, err := c.ScheduleBatch(ctx,
		notasap.Item(payloadA{Msg: "A"}, notasap.WithIdempotencyKey("x")),
		notasap.Item(payloadA{Msg: "B"}, notasap.WithIdempotencyKey("x")),
	)
	s.Require().NoError(err)
	s.Require().Len(res, 2)
	s.NoError(res[0].Err)
	s.Equal(res[0].ID, res[1].ID)
	s.ErrorIs(res[1].Err, notasap.ErrDuplicate)
	s.JSONEq(`{"msg":"A","n":0}`, string(s.Job(res[0].ID).Payload))

	res, err = c.ScheduleBatch(ctx,
		notasap.Item(payloadA{Msg: "A"}, notasap.WithIdempotencyKey("y"), notasap.WithReplace()),
		notasap.Item(payloadA{Msg: "B"}, notasap.WithIdempotencyKey("y"), notasap.WithReplace()),
	)
	s.Require().NoError(err)
	s.Require().Len(res, 2)
	s.NoError(res[0].Err)
	s.NoError(res[1].Err)
	s.Equal(res[0].ID, res[1].ID)
	s.JSONEq(`{"msg":"B","n":0}`, string(s.Job(res[0].ID).Payload))
	s.Equal(2, countJobs(&s.Suite))
}
