package notasap_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type ScheduleSuite struct {
	clientSuite
}

func TestScheduleSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ScheduleSuite))
}

// client returns an unstarted client with kinds a (MaxAttempts 7) and b.
func (s *ScheduleSuite) client(opts ...notasap.Option) *testClient {
	s.T().Helper()
	return s.newClient([]kindReg{
		reg(noop[payloadA], notasap.KindConfig{Name: "a", MaxAttempts: 7}),
		reg(noop[payloadB], notasap.KindConfig{Name: "b"}),
	}, opts...)
}

func (s *ScheduleSuite) TestSchedule() {
	ctx := context.Background()
	c := s.client()

	s.Run("inserts row", func() {
		p := payloadA{Msg: "hello", N: 3}
		id, err := c.Schedule(ctx, p)
		s.Require().NoError(err)

		row := s.GetJob(id)
		s.Equal(int16(kindA), row.Kind)
		s.Equal(notasap.StatusPending, notasap.Status(row.Status))
		s.Equal(0, row.Attempt)
		s.Equal(7, row.MaxAttempts)
		s.Nil(row.Key)
		want, err := json.Marshal(p)
		s.Require().NoError(err)
		s.JSONEq(string(want), string(row.Payload))
		s.WithinDuration(s.DBNow(), row.RunAt, time.Second)
	})
	s.Run("delay", func() {
		id, err := c.Schedule(ctx, payloadA{}, notasap.WithDelay(time.Hour))
		s.Require().NoError(err)

		row := s.GetJob(id)
		d := row.RunAt.Sub(row.CreatedAt)
		s.GreaterOrEqual(d, time.Hour-time.Second)
		s.LessOrEqual(d, time.Hour+time.Second)
	})
	s.Run("unknown kind", func() {
		_, err := c.Schedule(ctx, unregisteredPayload{})
		s.Require().ErrorIs(err, notasap.ErrUnknownKind)
		s.Equal(0, s.CountJobs())
	})
	s.Run("observer counts inserts and replaces", func() {
		c := s.client()
		_, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
		s.Require().NoError(err)
		_, err = c.Schedule(ctx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
		s.Require().NoError(err)
		_, err = c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
		s.Require().ErrorIs(err, notasap.ErrDuplicate)

		s.Equal(2, c.obs.Count("scheduled/a"))
		s.Equal(0, c.obs.Count("scheduled/b"))
	})
}

func (s *ScheduleSuite) TestScheduleTx() {
	ctx := context.Background()
	c := s.client()

	s.Run("commit", func() {
		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		id, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "commit"})
		s.Require().NoError(err)
		s.Require().NoError(tx.Commit(ctx))
		s.Equal(notasap.StatusPending, s.status(id))
	})
	s.Run("rollback", func() {
		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		_, err = c.ScheduleTx(ctx, tx, payloadA{Msg: "rollback"})
		s.Require().NoError(err)
		s.Require().NoError(tx.Rollback(ctx))
		s.Equal(0, s.CountJobs())
	})
}

// seedKey schedules a job with key and moves it into status st after an
// attempt that failed, as a worker would leave it.
func (s *ScheduleSuite) seedKey(c *testClient, key string, st notasap.Status) int64 {
	s.T().Helper()
	id, err := c.Schedule(context.Background(), payloadA{Msg: "old"}, notasap.WithIdempotencyKey(key))
	s.Require().NoError(err)
	s.Fixture.SetJob(context.Background(), id, pgtest.JobOpt{
		Status:      lo.ToPtr(int16(st)),
		Attempt:     lo.ToPtr(2),
		MaxAttempts: lo.ToPtr(99),
		Error:       lo.ToPtr("boom"),
		DeadlineIn:  lo.ToPtr(time.Duration(0)),
	})
	return id
}

func (s *ScheduleSuite) TestIdempotency() {
	ctx := context.Background()
	c := s.client()
	all := []notasap.Status{
		notasap.StatusPending, notasap.StatusRunning, notasap.StatusDone,
		notasap.StatusFailed, notasap.StatusCancelled,
	}

	s.Run("duplicate keeps existing row", func() {
		for _, st := range all {
			s.Run(st.String(), func() {
				origID := s.seedKey(c, "k", st)

				id, err := c.Schedule(ctx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"))
				s.Require().ErrorIs(err, notasap.ErrDuplicate)
				s.Equal(origID, id)
				row := s.GetJob(origID)
				s.Equal(st, notasap.Status(row.Status))
				s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
				s.Equal(1, s.CountJobs())
			})
		}
	})
	s.Run("replace resets the row", func() {
		for _, st := range []notasap.Status{
			notasap.StatusPending, notasap.StatusDone, notasap.StatusFailed, notasap.StatusCancelled,
		} {
			s.Run(st.String(), func() {
				origID := s.seedKey(c, "k", st)

				id, err := c.Schedule(ctx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
				s.Require().NoError(err)
				s.Equal(origID, id)
				row := s.GetJob(origID)
				s.Equal(notasap.StatusPending, notasap.Status(row.Status))
				s.Equal(0, row.Attempt)
				s.Equal(7, row.MaxAttempts)
				s.Nil(row.Error)
				s.Nil(row.Deadline)
				s.JSONEq(`{"msg":"new","n":0}`, string(row.Payload))
				s.Equal(1, s.CountJobs())
			})
		}
	})
	s.Run("replace running is refused", func() {
		origID := s.seedKey(c, "k", notasap.StatusRunning)

		id, err := c.Schedule(ctx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
		s.Require().ErrorIs(err, notasap.ErrJobRunning)
		s.Equal(origID, id)
		row := s.GetJob(origID)
		s.Equal(notasap.StatusRunning, notasap.Status(row.Status))
		s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
	})
	s.Run("replace running takes no lock", func() {
		id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey("k"))
		s.Require().NoError(err)
		s.Fixture.SetJob(ctx, id, pgtest.JobOpt{
			Status: lo.ToPtr(int16(notasap.StatusRunning)),
			Lease:  lo.ToPtr(int64(1)),
		})

		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
		s.Require().ErrorIs(err, notasap.ErrJobRunning)
		s.Equal(id, got)

		// The worker's status write must not wait for tx.
		finishCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		ok, err := store.New(s.Pool).Finish(finishCtx, store.Ref{ID: id, Lease: 1}, store.StatusDone, "")
		s.Require().NoError(err)
		s.True(ok)
	})
	s.Run("replace rolled back", func() {
		id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey("k"))
		s.Require().NoError(err)
		s.Fixture.SetJob(ctx, id, pgtest.JobOpt{
			Status: lo.ToPtr(int16(notasap.StatusFailed)),
			Error:  lo.ToPtr("boom"),
		})

		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		got, err := c.ScheduleTx(ctx, tx, payloadA{Msg: "new"}, notasap.WithIdempotencyKey("k"), notasap.WithReplace())
		s.Require().NoError(err)
		s.Equal(id, got)
		s.Require().NoError(tx.Rollback(ctx))

		row := s.GetJob(id)
		s.Equal(notasap.StatusFailed, notasap.Status(row.Status))
		s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
	})
	s.Run("same key different kind", func() {
		idA, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("k"))
		s.Require().NoError(err)
		idB, err := c.Schedule(ctx, payloadB{}, notasap.WithIdempotencyKey("k"))
		s.Require().NoError(err)
		s.NotEqual(idA, idB)
		s.Equal(2, s.CountJobs())
	})
	s.Run("concurrent", func() {
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
		s.Equal(1, s.CountJobs())
	})
	s.Run("duplicate takes one round trip", func() {
		id, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
		s.Require().NoError(err)

		db := countingDB{Pool: s.Pool, calls: new(atomic.Int32)}
		counted, err := notasap.NewWithDB(s.Pool, db)
		s.Require().NoError(err)
		s.Require().NoError(notasap.Register(counted, noop[payloadA], notasap.KindConfig{Name: "a"}))

		got, err := counted.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
		s.Require().ErrorIs(err, notasap.ErrDuplicate)
		s.Equal(id, got)
		s.Equal(int32(1), db.calls.Load())
	})
}

func (s *ScheduleSuite) TestScheduleBatch() {
	ctx := context.Background()
	c := s.client()

	s.Run("outcomes per item", func() {
		dupID, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("dup"))
		s.Require().NoError(err)
		runID, err := c.Schedule(ctx, payloadA{}, notasap.WithIdempotencyKey("run"))
		s.Require().NoError(err)
		s.Fixture.SetJob(ctx, runID, pgtest.JobOpt{Status: lo.ToPtr(int16(notasap.StatusRunning))})

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
		s.Equal(3, s.CountJobs())
	})
	s.Run("atomic on error", func() {
		_, err := c.ScheduleBatch(ctx,
			notasap.Item(payloadA{Msg: "one"}),
			notasap.Item(payloadA{Msg: "\x00"}),
			notasap.Item(payloadA{Msg: "three"}),
		)
		s.Require().ErrorContains(err, "unsupported Unicode escape")
		s.Equal(0, s.CountJobs())
	})
	s.Run("atomic on lookup error", func() {
		// The key is committed while the batch waits on it, after the batch
		// statement's snapshot, so the batch falls back to the lookup round.
		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = c.ScheduleTx(ctx, tx, payloadA{}, notasap.WithIdempotencyKey("dup"))
		s.Require().NoError(err)

		db := failingBatchDB{Pool: s.Pool, failAt: 2, calls: new(atomic.Int32)}
		failing, err := notasap.NewWithDB(s.Pool, db)
		s.Require().NoError(err)
		s.Require().NoError(notasap.Register(failing, noop[payloadA], notasap.KindConfig{Name: "a"}))

		var (
			batchErr error
			wg       sync.WaitGroup
		)
		wg.Go(func() {
			_, batchErr = failing.ScheduleBatch(ctx,
				notasap.Item(payloadA{Msg: "new"}),
				notasap.Item(payloadA{}, notasap.WithIdempotencyKey("dup")),
			)
		})
		s.WaitBlockedBy(s.BackendPID(tx))
		s.Require().NoError(tx.Commit(ctx))
		wg.Wait()
		s.Require().ErrorIs(batchErr, context.Canceled)
		s.Equal(int32(2), db.calls.Load())
		s.Equal(1, s.CountJobs())
	})
	s.Run("tx rollback", func() {
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
		s.Equal(0, s.CountJobs())

		res, err := c.ScheduleBatch(ctx, items...)
		s.Require().NoError(err)
		s.Require().Len(res, 3)
		for _, r := range res {
			s.NoError(r.Err)
		}
		s.Equal(3, s.CountJobs())
	})
	s.Run("same key twice", func() {
		res, err := c.ScheduleBatch(ctx,
			notasap.Item(payloadA{Msg: "A"}, notasap.WithIdempotencyKey("x")),
			notasap.Item(payloadA{Msg: "B"}, notasap.WithIdempotencyKey("x")),
		)
		s.Require().NoError(err)
		s.Require().Len(res, 2)
		s.NoError(res[0].Err)
		s.Equal(res[0].ID, res[1].ID)
		s.ErrorIs(res[1].Err, notasap.ErrDuplicate)
		s.JSONEq(`{"msg":"A","n":0}`, string(s.GetJob(res[0].ID).Payload))
		s.Equal(1, s.CountJobs())
	})
	s.Run("same key twice with replace", func() {
		res, err := c.ScheduleBatch(ctx,
			notasap.Item(payloadA{Msg: "A"}, notasap.WithIdempotencyKey("y"), notasap.WithReplace()),
			notasap.Item(payloadA{Msg: "B"}, notasap.WithIdempotencyKey("y"), notasap.WithReplace()),
		)
		s.Require().NoError(err)
		s.Require().Len(res, 2)
		s.NoError(res[0].Err)
		s.NoError(res[1].Err)
		s.Equal(res[0].ID, res[1].ID)
		s.JSONEq(`{"msg":"B","n":0}`, string(s.GetJob(res[0].ID).Payload))
		s.Equal(1, s.CountJobs())
	})
	s.Run("opposite key order does not deadlock", func() {
		resB, idA1, idA2 := s.oppositeKeyOrder(c)
		s.Equal(idA2, resB[0].ID)
		s.Equal(idA1, resB[1].ID)
		for _, r := range resB {
			s.ErrorIs(r.Err, notasap.ErrDuplicate)
		}
	})
	s.Run("opposite key order with replace does not deadlock", func() {
		for _, k := range []string{"k1", "k2"} {
			id, err := c.Schedule(ctx, payloadA{Msg: "old"}, notasap.WithIdempotencyKey(k))
			s.Require().NoError(err)
			s.Fixture.SetJob(ctx, id, pgtest.JobOpt{Status: lo.ToPtr(int16(notasap.StatusDone))})
		}
		resB, idA1, idA2 := s.oppositeKeyOrder(c, notasap.WithReplace())
		s.Equal(idA2, resB[0].ID)
		s.Equal(idA1, resB[1].ID)
		for _, r := range resB {
			s.NoError(r.Err)
		}
	})
}

// oppositeKeyOrder schedules keys k1 then k2 in transaction A and a batch
// of k2, k1 in transaction B that blocks on A, commits both, and returns B's
// results and A's ids for k1 and k2. Neither side may fail with a deadlock,
// and each key ends up in one row.
func (s *ScheduleSuite) oppositeKeyOrder(c *testClient, extra ...notasap.ScheduleOption) (resB []notasap.BatchResult, idA1, idA2 int64) {
	s.T().Helper()
	ctx := context.Background()
	opts := func(key string) []notasap.ScheduleOption {
		return append([]notasap.ScheduleOption{notasap.WithIdempotencyKey(key)}, extra...)
	}
	txA, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = txA.Rollback(ctx) }()
	txB, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = txB.Rollback(ctx) }()

	idA1, err = c.ScheduleTx(ctx, txA, payloadA{Msg: "A"}, opts("k1")...)
	s.Require().NoError(err)

	var (
		errB error
		wg   sync.WaitGroup
	)
	wg.Go(func() {
		resB, errB = c.ScheduleBatchTx(ctx, txB,
			notasap.Item(payloadA{Msg: "B"}, opts("k2")...),
			notasap.Item(payloadA{Msg: "B"}, opts("k1")...),
		)
	})
	s.WaitBlockedBy(s.BackendPID(txA))

	idA2, errA := c.ScheduleTx(ctx, txA, payloadA{Msg: "A"}, opts("k2")...)
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
	for _, k := range []string{"k1", "k2"} {
		var n int
		s.Require().NoError(s.Pool.QueryRow(ctx,
			"SELECT count(*) FROM notasap_jobs WHERE idempotency_key = $1", k).Scan(&n))
		s.Equal(1, n)
	}
	return resB, idA1, idA2
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
