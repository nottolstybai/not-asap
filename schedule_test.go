package notasap

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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type failStore struct {
	jobStore
	t *testing.T
}

func (f failStore) Insert(context.Context, store.DB, []store.InsertParams) ([]store.InsertResult, error) {
	f.t.Fatal("store must not be called")
	return nil, nil
}

func TestScheduleOptionsValidation(t *testing.T) {
	t.Parallel()
	c, err := newClient(failStore{t: t}, nil)
	require.NoError(t, err)
	require.NoError(t, Register(c, noop[testPayload], KindConfig{Name: "a"}))
	ctx := context.Background()

	t.Run("negative delay", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, testPayload{}, WithDelay(-time.Second))
		require.ErrorContains(t, err, "delay must not be negative")
	})
	t.Run("empty key", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, testPayload{}, WithIdempotencyKey(""))
		require.ErrorContains(t, err, "key is empty")
	})
	t.Run("replace without key", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, testPayload{}, WithReplace())
		require.ErrorContains(t, err, "notasap: Schedule: WithReplace requires WithIdempotencyKey")
	})
	t.Run("first error wins", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, testPayload{}, WithDelay(-time.Second), WithIdempotencyKey(""))
		require.ErrorContains(t, err, "WithDelay: delay must not be negative")
	})
	t.Run("unknown kind", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, unknownPayload{})
		require.ErrorIs(t, err, ErrUnknownKind)
		require.ErrorContains(t, err, "99")
	})
	t.Run("nil payload", func(t *testing.T) {
		t.Parallel()
		_, err := c.Schedule(ctx, nil)
		require.ErrorContains(t, err, "notasap: Schedule: payload is nil")
	})
	t.Run("batch rejected as a whole", func(t *testing.T) {
		t.Parallel()
		_, err := c.ScheduleBatch(ctx, Item(testPayload{}), Item(unknownPayload{}))
		require.ErrorIs(t, err, ErrUnknownKind)
	})
	t.Run("nil tx", func(t *testing.T) {
		t.Parallel()
		_, err := c.ScheduleTx(ctx, nil, testPayload{})
		require.ErrorContains(t, err, "tx is nil")
	})
	t.Run("empty batch", func(t *testing.T) {
		t.Parallel()
		res, err := c.ScheduleBatch(ctx)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}

type scheduleSuite struct {
	testSuite
}

func TestSchedule(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(scheduleSuite))
}

// client returns an unstarted client with kinds a (MaxAttempts 7) and b.
func (s *scheduleSuite) client(opts ...Option) *testClient {
	s.T().Helper()
	return s.newClient([]kindReg{
		reg(noop[payloadA], KindConfig{
			Name:        "a",
			MaxAttempts: 7,
		}),
		reg(noop[payloadB], KindConfig{Name: "b"}),
	}, opts...)
}

func (s *scheduleSuite) TestClient_Schedule() {
	c := s.client()

	s.Run("inserts row", func() {
		p := payloadA{
			Msg: "hello",
			N:   3,
		}
		id, err := c.Schedule(s.ctx, p)
		s.Require().NoError(err)

		row := s.GetJob(id)
		s.Equal(int16(kindA), row.Kind)
		s.Equal(StatusPending, Status(row.Status))
		s.Equal(0, row.Attempt)
		s.Equal(7, row.MaxAttempts)
		s.Nil(row.Key)
		want, err := json.Marshal(p)
		s.Require().NoError(err)
		s.JSONEq(string(want), string(row.Payload))
		s.WithinDuration(s.DBNow(), row.RunAt, time.Second)
	})
	s.Run("delay", func() {
		id, err := c.Schedule(s.ctx, payloadA{}, WithDelay(time.Hour))
		s.Require().NoError(err)

		row := s.GetJob(id)
		d := row.RunAt.Sub(row.CreatedAt)
		s.GreaterOrEqual(d, time.Hour-time.Second)
		s.LessOrEqual(d, time.Hour+time.Second)
	})
	s.Run("unknown kind", func() {
		_, err := c.Schedule(s.ctx, unknownPayload{})
		s.ErrorIs(err, ErrUnknownKind)
		s.Equal(0, s.CountJobs())
	})
	s.Run("observer counts inserts and replaces", func() {
		c := s.client()
		_, err := c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("k"))
		s.Require().NoError(err)
		_, err = c.Schedule(s.ctx, payloadA{Msg: "new"}, WithIdempotencyKey("k"), WithReplace())
		s.Require().NoError(err)
		_, err = c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("k"))
		s.ErrorIs(err, ErrDuplicate)

		s.Equal(2, c.obs.Count("scheduled/a"))
		s.Equal(0, c.obs.Count("scheduled/b"))
	})
}

func (s *scheduleSuite) TestClient_ScheduleTx() {
	c := s.client()

	s.Run("commit", func() {
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		id, err := c.ScheduleTx(s.ctx, tx, payloadA{Msg: "commit"})
		s.Require().NoError(err)
		s.Require().NoError(tx.Commit(s.ctx))
		s.Equal(StatusPending, Status(s.GetJob(id).Status))
	})
	s.Run("rollback", func() {
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		_, err = c.ScheduleTx(s.ctx, tx, payloadA{Msg: "rollback"})
		s.Require().NoError(err)
		s.Require().NoError(tx.Rollback(s.ctx))
		s.Equal(0, s.CountJobs())
	})
}

// seedKey schedules a job with key and moves it into status st after an
// attempt that failed, as a worker would leave it.
func (s *scheduleSuite) seedKey(c *testClient, key string, st Status) int64 {
	s.T().Helper()
	id, err := c.Schedule(s.ctx, payloadA{Msg: "old"}, WithIdempotencyKey(key))
	s.Require().NoError(err)
	s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{
		Status:      lo.ToPtr(int16(st)),
		Attempt:     lo.ToPtr(2),
		MaxAttempts: lo.ToPtr(99),
		Error:       lo.ToPtr("boom"),
		DeadlineIn:  lo.ToPtr(time.Duration(0)),
	})
	return id
}

func (s *scheduleSuite) TestClient_Schedule_Idempotency() {
	c := s.client()
	all := []Status{
		StatusPending, StatusRunning, StatusDone,
		StatusFailed, StatusCancelled,
	}

	s.Run("duplicate keeps existing row", func() {
		for _, st := range all {
			s.Run(st.String(), func() {
				origID := s.seedKey(c, "k", st)

				id, err := c.Schedule(s.ctx, payloadA{Msg: "new"}, WithIdempotencyKey("k"))
				s.ErrorIs(err, ErrDuplicate)
				s.Equal(origID, id)
				row := s.GetJob(origID)
				s.Equal(st, Status(row.Status))
				s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
				s.Equal(1, s.CountJobs())
			})
		}
	})
	s.Run("replace resets the row", func() {
		for _, st := range []Status{
			StatusPending, StatusDone, StatusFailed, StatusCancelled,
		} {
			s.Run(st.String(), func() {
				origID := s.seedKey(c, "k", st)

				id, err := c.Schedule(s.ctx, payloadA{Msg: "new"}, WithIdempotencyKey("k"), WithReplace())
				s.Require().NoError(err)
				s.Equal(origID, id)
				row := s.GetJob(origID)
				s.Equal(StatusPending, Status(row.Status))
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
		origID := s.seedKey(c, "k", StatusRunning)

		id, err := c.Schedule(s.ctx, payloadA{Msg: "new"}, WithIdempotencyKey("k"), WithReplace())
		s.ErrorIs(err, ErrJobRunning)
		s.Equal(origID, id)
		row := s.GetJob(origID)
		s.Equal(StatusRunning, Status(row.Status))
		s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
	})
	s.Run("replace running takes no lock", func() {
		id, err := c.Schedule(s.ctx, payloadA{Msg: "old"}, WithIdempotencyKey("k"))
		s.Require().NoError(err)
		s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{
			Status: lo.ToPtr(int16(StatusRunning)),
			Lease:  lo.ToPtr(int64(1)),
		})

		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		got, err := c.ScheduleTx(s.ctx, tx, payloadA{Msg: "new"}, WithIdempotencyKey("k"), WithReplace())
		s.ErrorIs(err, ErrJobRunning)
		s.Equal(id, got)

		// The worker's status write must not wait for tx.
		finishCtx, cancel := context.WithTimeout(s.ctx, time.Second)
		defer cancel()
		written, err := store.New(s.Pool).Complete(finishCtx, []store.Completion{{
			Ref: store.Ref{
				ID:    id,
				Lease: 1,
			},
			Status: store.StatusDone,
		}})
		s.Require().NoError(err)
		s.Len(written, 1)
	})
	s.Run("replace rolled back", func() {
		id, err := c.Schedule(s.ctx, payloadA{Msg: "old"}, WithIdempotencyKey("k"))
		s.Require().NoError(err)
		s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{
			Status: lo.ToPtr(int16(StatusFailed)),
			Error:  lo.ToPtr("boom"),
		})

		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		got, err := c.ScheduleTx(s.ctx, tx, payloadA{Msg: "new"}, WithIdempotencyKey("k"), WithReplace())
		s.Require().NoError(err)
		s.Equal(id, got)
		s.Require().NoError(tx.Rollback(s.ctx))

		row := s.GetJob(id)
		s.Equal(StatusFailed, Status(row.Status))
		s.JSONEq(`{"msg":"old","n":0}`, string(row.Payload))
	})
	s.Run("same key different kind", func() {
		idA, err := c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("k"))
		s.Require().NoError(err)
		idB, err := c.Schedule(s.ctx, payloadB{}, WithIdempotencyKey("k"))
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
				ids[i], errs[i] = c.Schedule(s.ctx, payloadA{N: i}, WithIdempotencyKey("same"))
			})
		}
		close(start)
		wg.Wait()

		var ok, dup int
		for i := range n {
			switch {
			case errs[i] == nil:
				ok++
			case s.ErrorIs(errs[i], ErrDuplicate):
				dup++
			}
			s.Equal(ids[0], ids[i])
		}
		s.Equal(1, ok)
		s.Equal(n-1, dup)
		s.Equal(1, s.CountJobs())
	})
	s.Run("duplicate takes one round trip", func() {
		id, err := c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("dup"))
		s.Require().NoError(err)

		db := countingDB{
			Pool:  s.Pool,
			calls: new(atomic.Int32),
		}
		counted, err := newClient(store.New(s.Pool), db)
		s.Require().NoError(err)
		s.Require().NoError(Register(counted, noop[payloadA], KindConfig{Name: "a"}))

		got, err := counted.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("dup"))
		s.ErrorIs(err, ErrDuplicate)
		s.Equal(id, got)
		s.Equal(int32(1), db.calls.Load())
	})
}

func (s *scheduleSuite) TestClient_ScheduleBatch() {
	c := s.client()

	s.Run("outcomes per item", func() {
		dupID, err := c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("dup"))
		s.Require().NoError(err)
		runID, err := c.Schedule(s.ctx, payloadA{}, WithIdempotencyKey("run"))
		s.Require().NoError(err)
		s.Fixture.SetJob(s.ctx, runID, pgtest.JobOpt{Status: lo.ToPtr(int16(StatusRunning))})

		res, err := c.ScheduleBatch(s.ctx,
			Item(payloadA{Msg: "new"}),
			Item(payloadA{}, WithIdempotencyKey("dup")),
			Item(payloadA{}, WithIdempotencyKey("run"), WithReplace()),
		)
		s.Require().NoError(err)
		s.Require().Len(res, 3)
		s.NoError(res[0].Err)
		s.NotZero(res[0].ID)
		s.NotEqual(dupID, res[0].ID)
		s.NotEqual(runID, res[0].ID)
		s.Equal(dupID, res[1].ID)
		s.ErrorIs(res[1].Err, ErrDuplicate)
		s.Equal(runID, res[2].ID)
		s.ErrorIs(res[2].Err, ErrJobRunning)
		s.Equal(3, s.CountJobs())
	})
	s.Run("atomic on error", func() {
		_, err := c.ScheduleBatch(s.ctx,
			Item(payloadA{Msg: "one"}),
			Item(payloadA{Msg: "\x00"}),
			Item(payloadA{Msg: "three"}),
		)
		s.ErrorContains(err, "unsupported Unicode escape")
		s.Equal(0, s.CountJobs())
	})
	s.Run("atomic on lookup error", func() {
		// The key is committed while the batch waits on it, after the batch
		// statement's snapshot, so the batch falls back to the lookup round.
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		_, err = c.ScheduleTx(s.ctx, tx, payloadA{}, WithIdempotencyKey("dup"))
		s.Require().NoError(err)

		db := failingBatchDB{
			Pool:   s.Pool,
			failAt: 2,
			calls:  new(atomic.Int32),
		}
		failing, err := newClient(store.New(s.Pool), db)
		s.Require().NoError(err)
		s.Require().NoError(Register(failing, noop[payloadA], KindConfig{Name: "a"}))

		var (
			batchErr error
			wg       sync.WaitGroup
		)
		wg.Go(func() {
			_, batchErr = failing.ScheduleBatch(s.ctx,
				Item(payloadA{Msg: "new"}),
				Item(payloadA{}, WithIdempotencyKey("dup")),
			)
		})
		s.WaitBlockedBy(s.BackendPID(tx))
		s.Require().NoError(tx.Commit(s.ctx))
		wg.Wait()
		s.ErrorIs(batchErr, context.Canceled)
		s.Equal(int32(2), db.calls.Load())
		s.Equal(1, s.CountJobs())
	})
	s.Run("tx rollback", func() {
		items := []BatchItem{
			Item(payloadA{N: 1}),
			Item(payloadA{N: 2}),
			Item(payloadB{}),
		}
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		_, err = c.ScheduleBatchTx(s.ctx, tx, items...)
		s.Require().NoError(err)
		s.Require().NoError(tx.Rollback(s.ctx))
		s.Equal(0, s.CountJobs())

		res, err := c.ScheduleBatch(s.ctx, items...)
		s.Require().NoError(err)
		s.Require().Len(res, 3)
		for _, r := range res {
			s.NoError(r.Err)
		}
		s.Equal(3, s.CountJobs())
	})
	s.Run("same key twice", func() {
		res, err := c.ScheduleBatch(s.ctx,
			Item(payloadA{Msg: "A"}, WithIdempotencyKey("x")),
			Item(payloadA{Msg: "B"}, WithIdempotencyKey("x")),
		)
		s.Require().NoError(err)
		s.Require().Len(res, 2)
		s.NoError(res[0].Err)
		s.Equal(res[0].ID, res[1].ID)
		s.ErrorIs(res[1].Err, ErrDuplicate)
		s.JSONEq(`{"msg":"A","n":0}`, string(s.GetJob(res[0].ID).Payload))
		s.Equal(1, s.CountJobs())
	})
	s.Run("same key twice with replace", func() {
		res, err := c.ScheduleBatch(s.ctx,
			Item(payloadA{Msg: "A"}, WithIdempotencyKey("y"), WithReplace()),
			Item(payloadA{Msg: "B"}, WithIdempotencyKey("y"), WithReplace()),
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
			s.ErrorIs(r.Err, ErrDuplicate)
		}
	})
	s.Run("opposite key order with replace does not deadlock", func() {
		for _, k := range []string{"k1", "k2"} {
			id, err := c.Schedule(s.ctx, payloadA{Msg: "old"}, WithIdempotencyKey(k))
			s.Require().NoError(err)
			s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{Status: lo.ToPtr(int16(StatusDone))})
		}
		resB, idA1, idA2 := s.oppositeKeyOrder(c, WithReplace())
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
func (s *scheduleSuite) oppositeKeyOrder(c *testClient, extra ...ScheduleOption) (resB []BatchResult, idA1, idA2 int64) {
	s.T().Helper()
	opts := func(key string) []ScheduleOption {
		return append([]ScheduleOption{WithIdempotencyKey(key)}, extra...)
	}
	txA, err := s.Pool.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { _ = txA.Rollback(s.ctx) }()
	txB, err := s.Pool.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { _ = txB.Rollback(s.ctx) }()

	idA1, err = c.ScheduleTx(s.ctx, txA, payloadA{Msg: "A"}, opts("k1")...)
	s.Require().NoError(err)

	var (
		errB error
		wg   sync.WaitGroup
	)
	wg.Go(func() {
		resB, errB = c.ScheduleBatchTx(s.ctx, txB,
			Item(payloadA{Msg: "B"}, opts("k2")...),
			Item(payloadA{Msg: "B"}, opts("k1")...),
		)
	})
	s.WaitBlockedBy(s.BackendPID(txA))

	idA2, errA := c.ScheduleTx(s.ctx, txA, payloadA{Msg: "A"}, opts("k2")...)
	if errA == nil {
		errA = txA.Commit(s.ctx)
	}
	wg.Wait()
	if errB == nil {
		errB = txB.Commit(s.ctx)
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
		s.Require().NoError(s.Pool.QueryRow(s.ctx,
			"SELECT count(*) FROM notasap_jobs WHERE idempotency_key = $1", k).Scan(&n))
		s.Equal(1, n)
	}
	return resB, idA1, idA2
}

func (s *scheduleSuite) TestClient_ScheduleTx_VisibleOnlyAfterCommit() {
	var called atomic.Bool
	c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
		called.Store(true)
		return nil
	}, KindConfig{Name: "a"})})

	tx, err := s.Pool.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(s.ctx) }()
	id, err := c.ScheduleTx(s.ctx, tx, payloadA{})
	s.Require().NoError(err)
	// A claim from another connection must not see the uncommitted row.
	other := store.New(pgtest.NewPool(s.T(), s.Schema))
	jobs, err := other.Claim(s.ctx, int16(kindA), 10, time.Minute)
	s.Require().NoError(err)
	s.Empty(jobs)
	s.False(called.Load())

	s.Require().NoError(tx.Commit(s.ctx))
	pgtest.WaitStatus(&s.Suite, id, StatusDone)
	s.True(called.Load())
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
	return failingBatchTx{
		Tx:     tx,
		failAt: d.failAt,
		calls:  d.calls,
	}, nil
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
