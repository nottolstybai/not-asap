package notasap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

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
