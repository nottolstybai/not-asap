package notasap_test

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type JanitorSuite struct {
	pgtest.Suite
}

func TestJanitorSuite(t *testing.T) {
	suite.Run(t, new(JanitorSuite))
}

// janitorObserver records JanitorDeleted calls.
type janitorObserver struct {
	notasap.NopObserver
	mu      sync.Mutex
	batches []int64
}

func (o *janitorObserver) JanitorDeleted(_ notasap.Status, n int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batches = append(o.batches, n)
}

// Batches returns the batch sizes reported so far.
func (o *janitorObserver) Batches() []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int64(nil), o.batches...)
}

// startClient starts a client with a fast janitor config merged with cfg.
func (s *JanitorSuite) startClient(cfg notasap.JanitorConfig, obs notasap.Observer) *notasap.Client {
	s.T().Helper()
	if cfg.Interval == 0 {
		cfg.Interval = 20 * time.Millisecond
	}
	c, err := notasap.New(s.Pool,
		notasap.WithLogger(slog.New(slog.DiscardHandler)),
		notasap.WithObserver(obs),
		notasap.WithJanitor(cfg),
	)
	s.Require().NoError(err)
	s.Require().NoError(c.Start(context.Background()))
	s.T().Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.NoError(c.Stop(ctx))
	})
	return c
}

// insertFinished inserts a row in status whose updated_at is age in the past.
func (s *JanitorSuite) insertFinished(status notasap.Status, age time.Duration) int64 {
	s.T().Helper()
	var id int64
	s.Require().NoError(s.Pool.QueryRow(context.Background(), `
INSERT INTO notasap_jobs (kind, status, payload, max_attempts, updated_at)
VALUES (1, $1, '{}', 3, NOW() - make_interval(secs => $2)) RETURNING id`,
		int16(status), age.Seconds()).Scan(&id))
	return id
}

func (s *JanitorSuite) exists(id int64) bool {
	s.T().Helper()
	var ok bool
	s.Require().NoError(s.Pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM notasap_jobs WHERE id = $1)", id).Scan(&ok))
	return ok
}

// settle waits until the rows in gone are deleted, then checks that the rows
// in kept are still there after a short quiet period.
func (s *JanitorSuite) settle(gone, kept []int64) {
	s.T().Helper()
	s.Require().Eventually(func() bool {
		return !slices.ContainsFunc(gone, s.exists)
	}, waitFor, tick)
	s.Never(func() bool {
		for _, id := range kept {
			if !s.exists(id) {
				return true
			}
		}
		return false
	}, 200*time.Millisecond, tick)
}

func (s *JanitorSuite) TestDefaultRetention() {
	doneOld := s.insertFinished(notasap.StatusDone, 25*time.Hour)
	cancelledOld := s.insertFinished(notasap.StatusCancelled, 25*time.Hour)
	doneNew := s.insertFinished(notasap.StatusDone, time.Hour)
	failedOld := s.insertFinished(notasap.StatusFailed, 30*24*time.Hour)

	s.startClient(notasap.JanitorConfig{}, notasap.NopObserver{})
	s.settle([]int64{doneOld, cancelledOld}, []int64{doneNew, failedOld})
}

func (s *JanitorSuite) TestCustomRetentionMerged() {
	doneOld := s.insertFinished(notasap.StatusDone, 2*time.Minute)
	cancelledOld := s.insertFinished(notasap.StatusCancelled, 25*time.Hour)
	cancelledNew := s.insertFinished(notasap.StatusCancelled, time.Hour)

	s.startClient(notasap.JanitorConfig{
		Retention: map[notasap.Status]time.Duration{notasap.StatusDone: time.Minute},
	}, notasap.NopObserver{})
	s.settle([]int64{doneOld, cancelledOld}, []int64{cancelledNew})
}

func (s *JanitorSuite) TestBatches() {
	for range 25 {
		s.insertFinished(notasap.StatusDone, 25*time.Hour)
	}
	obs := &janitorObserver{}
	s.startClient(notasap.JanitorConfig{BatchSize: 10}, obs)

	s.Require().Eventually(func() bool { return countJobs(&s.Suite) == 0 }, waitFor, tick)
	s.Require().Eventually(func() bool { return len(obs.Batches()) >= 3 }, waitFor, tick)
	var sum int64
	for _, n := range obs.Batches() {
		s.Positive(n)
		sum += n
	}
	s.Equal(int64(25), sum)
}

func (s *JanitorSuite) TestDisabled() {
	s.insertFinished(notasap.StatusDone, 25*time.Hour)
	s.insertFinished(notasap.StatusDone, 26*time.Hour)

	s.startClient(notasap.JanitorConfig{Disabled: true}, notasap.NopObserver{})
	s.Never(func() bool { return countJobs(&s.Suite) < 2 }, 200*time.Millisecond, tick)
}

func (s *JanitorSuite) TestStopInterruptsJanitor() {
	s.Exec(`INSERT INTO notasap_jobs (kind, status, payload, max_attempts, updated_at)
SELECT 1, 3, '{}', 3, NOW() - interval '25 hours' FROM generate_series(1, 5000)`)

	c := s.startClient(notasap.JanitorConfig{BatchSize: 1, Interval: time.Hour}, notasap.NopObserver{})
	s.Require().Eventually(func() bool { return countJobs(&s.Suite) < 5000 }, waitFor, tick)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	s.Require().NoError(c.Stop(ctx))
	s.Less(time.Since(start), time.Second)
	s.Positive(countJobs(&s.Suite))
}
