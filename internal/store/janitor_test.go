package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type janitorSuite struct {
	testSuite
}

func TestJanitor(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(janitorSuite))
}

func (s *janitorSuite) TestStore_DeleteFinished() {
	oldest := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusDone),
		UpdatedAgo: lo.ToPtr(5 * time.Hour),
	}).ID
	older := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusDone),
		UpdatedAgo: lo.ToPtr(4 * time.Hour),
	}).ID
	old := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusDone),
		UpdatedAgo: lo.ToPtr(3 * time.Hour),
	}).ID
	fresh := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusDone),
		UpdatedAgo: lo.ToPtr(time.Minute),
	}).ID
	failed := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusFailed),
		UpdatedAgo: lo.ToPtr(5 * time.Hour),
	}).ID
	pending := s.Fixture.Job(s.ctx, pgtest.JobOpt{UpdatedAgo: lo.ToPtr(5 * time.Hour)}).ID

	n, err := s.st.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(2), n)
	s.False(s.Exists(oldest))
	s.False(s.Exists(older))
	s.True(s.Exists(old))

	n, err = s.st.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(1), n)
	s.False(s.Exists(old))

	n, err = s.st.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Zero(n)
	s.True(s.Exists(fresh))
	s.True(s.Exists(failed))
	s.True(s.Exists(pending))
}

// holdJanitorLock takes the janitor advisory lock of pool's schema in a tx
// that is rolled back on cleanup, unless the returned func is called first.
func (s *janitorSuite) holdJanitorLock(pool *pgxpool.Pool) (release func()) {
	tx, err := pool.Begin(s.ctx)
	s.Require().NoError(err)
	released := false
	release = func() {
		if !released {
			released = true
			s.NoError(tx.Rollback(context.WithoutCancel(s.ctx))) // may run on cleanup, after s.ctx is cancelled
		}
	}
	s.T().Cleanup(release)
	_, err = tx.Exec(s.ctx, `SELECT pg_advisory_xact_lock('notasap_jobs'::regclass::oid::int, 1)`)
	s.Require().NoError(err)
	return release
}

func (s *janitorSuite) TestStore_DeleteFinished_SkipsWhileLocked() {
	id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     lo.ToPtr(store.StatusDone),
		UpdatedAgo: lo.ToPtr(3 * time.Hour),
	}).ID
	release := s.holdJanitorLock(s.Pool)

	n, err := s.st.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 10)
	s.Require().NoError(err)
	s.Zero(n)
	s.True(s.Exists(id))

	release()
	n, err = s.st.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 10)
	s.Require().NoError(err)
	s.EqualValues(1, n)
	s.False(s.Exists(id))
}

func (s *janitorSuite) TestStore_DeleteFinished_IndependentSchemas() {
	_, otherPool := pgtest.NewSchema(s.T())
	otherSt := store.New(otherPool)
	_, err := otherPool.Exec(s.ctx, `INSERT INTO notasap_jobs (kind, status, payload, max_attempts, run_at, updated_at)
VALUES (1, $1, '{}', 3, NOW(), NOW() - interval '3 hours')`, store.StatusDone)
	s.Require().NoError(err)
	s.holdJanitorLock(s.Pool)

	n, err := otherSt.DeleteFinished(s.ctx, store.StatusDone, time.Hour, 10)
	s.Require().NoError(err)
	s.EqualValues(1, n)
}
