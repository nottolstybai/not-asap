package store_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type insertSuite struct {
	testSuite
}

func TestInsert(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(insertSuite))
}

func (s *insertSuite) TestStore_Insert_OutcomesInParamsOrder() {
	key := func(k string) *string { return &k }
	seed, err := s.st.Insert(s.ctx, s.Pool, []store.InsertParams{
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("done")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("run")},
	})
	s.Require().NoError(err)
	s.Fixture.SetJob(s.ctx, seed[1].ID, pgtest.JobOpt{Status: lo.ToPtr(store.StatusDone)})
	s.Fixture.SetJob(s.ctx, seed[2].ID, pgtest.JobOpt{Status: lo.ToPtr(store.StatusRunning)})

	res, err := s.st.Insert(s.ctx, s.Pool, []store.InsertParams{
		{Kind: 2, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("run"), Replace: true},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("done"), Replace: true},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
	})
	s.Require().NoError(err)
	s.Require().Len(res, 5)
	s.Equal(store.Inserted, res[0].Outcome)
	s.Equal(store.InsertResult{
		ID:      seed[2].ID,
		Outcome: store.Running,
	}, res[1])
	s.Equal(store.Inserted, res[2].Outcome)
	s.Equal(store.InsertResult{
		ID:      seed[1].ID,
		Outcome: store.Replaced,
	}, res[3])
	s.Equal(store.InsertResult{
		ID:      seed[0].ID,
		Outcome: store.Duplicate,
	}, res[4])
	// The two inserted rows are new: non-zero, distinct, not a seed id.
	for _, r := range []store.InsertResult{res[0], res[2]} {
		s.NotZero(r.ID)
		for _, sd := range seed {
			s.NotEqual(sd.ID, r.ID)
		}
	}
	s.NotEqual(res[0].ID, res[2].ID)
	s.Equal(store.StatusPending, s.GetJob(seed[1].ID).Status)
	s.Equal(store.StatusRunning, s.GetJob(seed[2].ID).Status)
}

func (s *insertSuite) TestStore_Insert_KeylessBatch() {
	params := make([]store.InsertParams, 500)
	for i := range params {
		params[i] = store.InsertParams{
			Kind:        int16(3 - i%3),
			Payload:     fmt.Appendf(nil, `{"n":%d}`, i),
			MaxAttempts: i%7 + 1,
			Delay:       time.Duration(i) * time.Second,
		}
	}

	res, err := s.st.Insert(s.ctx, s.Pool, params)
	s.Require().NoError(err)
	s.Require().Len(res, len(params))
	now := s.DBNow()
	for i, r := range res {
		s.Equal(store.Inserted, r.Outcome)
		row := s.GetJob(r.ID)
		s.Equal(params[i].Kind, row.Kind)
		s.JSONEq(string(params[i].Payload), string(row.Payload))
		s.Equal(params[i].MaxAttempts, row.MaxAttempts)
		s.WithinDuration(now.Add(params[i].Delay), row.RunAt, 5*time.Second)
		s.Nil(row.Key)
	}

	// Rows are inserted ordered by kind, then params order.
	ids := lo.Map(res, func(r store.InsertResult, _ int) int64 { return r.ID })
	order := make([]int, len(params))
	for i := range order {
		order[i] = i
	}

	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(params[a].Kind, params[b].Kind) })
	for j := 1; j < len(order); j++ {
		s.Less(ids[order[j-1]], ids[order[j]])
	}

	s.Equal(len(params), s.CountJobs())
}

func (s *insertSuite) TestStore_Insert_MixedBatchOrder() {
	res, err := s.st.Insert(s.ctx, s.Pool, []store.InsertParams{
		{Kind: 2, Payload: []byte(`{"n":0}`), MaxAttempts: 3, Key: lo.ToPtr("b")},
		{Kind: 2, Payload: []byte(`{"n":1}`), MaxAttempts: 3},
		{Kind: 1, Payload: []byte(`{"n":2}`), MaxAttempts: 3, Key: lo.ToPtr("a")},
		{Kind: 2, Payload: []byte(`{"n":3}`), MaxAttempts: 3, Key: lo.ToPtr("a")},
		{Kind: 1, Payload: []byte(`{"n":4}`), MaxAttempts: 3},
		{Kind: 2, Payload: []byte(`{"n":5}`), MaxAttempts: 3},
	})
	s.Require().NoError(err)
	s.Require().Len(res, 6)
	for i, r := range res {
		s.Equal(store.Inserted, r.Outcome)
		s.JSONEq(fmt.Sprintf(`{"n":%d}`, i), string(s.GetJob(r.ID).Payload))
	}

	// Keyless rows first by kind, then params order; keyed rows by (kind, key).
	for _, pair := range [][2]int{{4, 1}, {1, 5}, {5, 2}, {2, 3}, {3, 0}} {
		s.Less(res[pair[0]].ID, res[pair[1]].ID)
	}
}

func (s *insertSuite) TestStore_Insert_AtomicOnError() {
	s.Run("keyless", func() {
		_, err := s.st.Insert(s.ctx, s.Pool, []store.InsertParams{
			{Kind: 1, Payload: []byte(`{}`), MaxAttempts: 3},
			{Kind: 1, Payload: []byte(`{"m":"\u0000"}`), MaxAttempts: 3},
		})
		s.ErrorContains(err, "unsupported Unicode escape")
		s.Equal(0, s.CountJobs())
	})
	s.Run("mixed", func() {
		_, err := s.st.Insert(s.ctx, s.Pool, []store.InsertParams{
			{Kind: 1, Payload: []byte(`{}`), MaxAttempts: 3},
			{Kind: 1, Payload: []byte(`{}`), MaxAttempts: 3, Key: lo.ToPtr("a")},
			{Kind: 1, Payload: []byte(`{"m":"\u0000"}`), MaxAttempts: 3, Key: lo.ToPtr("b")},
		})
		s.ErrorContains(err, "unsupported Unicode escape")
		s.Equal(0, s.CountJobs())
	})
}

func (s *insertSuite) TestStore_Insert_ReplaceClaimedMeanwhile() {
	key := "k"
	params := []store.InsertParams{{
		Kind:        1,
		Payload:     []byte(`{"new":true}`),
		MaxAttempts: 3,
		Key:         &key,
		Replace:     true,
	}}

	// claim marks the pending row of key running in a transaction left open,
	// like a worker's claim between the insert's snapshot and its conflict
	// check, and returns the transaction and the new lease.
	claim := func() (pgx.Tx, int64, int64) {
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind: lo.ToPtr(int16(1)),
			Key:  lo.ToPtr(key),
		}).ID
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		s.T().Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(s.ctx)) })
		var lease int64
		s.Require().NoError(tx.QueryRow(s.ctx,
			"UPDATE notasap_jobs SET status = 2, lease = lease + 1 WHERE id = $1 RETURNING lease", id).Scan(&lease))
		return tx, id, lease
	}

	s.Run("autocommit", func() {
		claimTx, id, lease := claim()
		var (
			res []store.InsertResult
			err error
			wg  sync.WaitGroup
		)
		wg.Go(func() { res, err = s.st.Insert(s.ctx, s.Pool, params) })
		s.WaitBlockedBy(s.BackendPID(claimTx))
		s.Require().NoError(claimTx.Commit(s.ctx))
		wg.Wait()
		s.Require().NoError(err)
		s.Equal([]store.InsertResult{{ID: id, Outcome: store.Running}}, res)

		// The running row is not left locked: the worker's status write completes.
		finishCtx, cancel := context.WithTimeout(s.ctx, time.Second)
		defer cancel()
		written, err := s.st.Complete(finishCtx, []store.Completion{{
			Ref: store.Ref{
				ID:    id,
				Lease: lease,
			},
			Status: store.StatusDone,
		}})
		s.Require().NoError(err)
		s.Len(written, 1)
	})
	s.Run("in caller tx", func() {
		claimTx, id, lease := claim()
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		var (
			res    []store.InsertResult
			insErr error
			wg     sync.WaitGroup
		)
		wg.Go(func() { res, insErr = s.st.Insert(s.ctx, tx, params) })
		s.WaitBlockedBy(s.BackendPID(claimTx))
		s.Require().NoError(claimTx.Commit(s.ctx))
		wg.Wait()
		s.Require().NoError(insErr)
		s.Equal([]store.InsertResult{{ID: id, Outcome: store.Running}}, res)

		// The conflict check locked the running row: the status write waits for tx.
		var (
			written []store.Ref
			finErr  error
		)
		wg.Go(func() {
			written, finErr = s.st.Complete(s.ctx, []store.Completion{{
				Ref: store.Ref{
					ID:    id,
					Lease: lease,
				},
				Status: store.StatusDone,
			}})
		})
		s.WaitBlockedBy(s.BackendPID(tx))
		s.Require().NoError(tx.Commit(s.ctx))
		wg.Wait()
		s.Require().NoError(finErr)
		s.Len(written, 1)
		s.JSONEq(`{}`, string(s.GetJob(id).Payload))
	})
}

func BenchmarkInsertBatch(b *testing.B) {
	_, pool := pgtest.NewSchema(b)
	st := store.New(pool)
	params := make([]store.InsertParams, 100)
	for i := range params {
		params[i] = store.InsertParams{
			Kind:        1,
			Payload:     []byte(`{"n":1}`),
			MaxAttempts: 3,
		}
	}

	for b.Loop() {
		if _, err := st.Insert(b.Context(), pool, params); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N*len(params))/b.Elapsed().Seconds(), "jobs/s")
}
