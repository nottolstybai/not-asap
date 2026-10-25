package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	s.Fixture.SetJob(s.ctx, seed[1].ID, pgtest.JobOpt{Status: new(store.StatusDone)})
	s.Fixture.SetJob(s.ctx, seed[2].ID, pgtest.JobOpt{Status: new(store.StatusRunning)})

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
			Kind: new(int16(1)),
			Key:  new(key),
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
