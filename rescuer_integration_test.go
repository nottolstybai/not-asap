package notasap_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type RescuerSuite struct {
	pgtest.Suite
}

func TestRescuerSuite(t *testing.T) {
	suite.Run(t, new(RescuerSuite))
}

// rescueObserver counts rescued jobs per kind name on top of workerObserver.
type rescueObserver struct {
	*workerObserver
	rmu     sync.Mutex
	rescued map[string]int
}

func newRescueObserver() *rescueObserver {
	return &rescueObserver{workerObserver: newWorkerObserver(), rescued: make(map[string]int)}
}

func (o *rescueObserver) JobRescued(kind string) {
	o.rmu.Lock()
	defer o.rmu.Unlock()
	o.rescued[kind]++
}

func (o *rescueObserver) Rescued(kind string) int {
	o.rmu.Lock()
	defer o.rmu.Unlock()
	return o.rescued[kind]
}

// startClient starts a client with a fast rescuer, client default backoff of
// 2h and the given kinds; it is stopped on test cleanup.
func (s *RescuerSuite) startClient(obs notasap.Observer, kinds ...kindReg) *notasap.Client {
	s.T().Helper()
	c, err := notasap.New(s.Pool,
		notasap.WithLogger(slog.New(slog.DiscardHandler)),
		notasap.WithObserver(obs),
		notasap.WithDefaults(notasap.KindConfig{PollInterval: 10 * time.Millisecond, Backoff: notasap.Constant(2 * time.Hour)}),
		notasap.WithRescuer(notasap.RescuerConfig{Interval: 20 * time.Millisecond, Grace: 20 * time.Millisecond}),
	)
	s.Require().NoError(err)
	for _, k := range kinds {
		s.Require().NoError(k(c))
	}
	s.Require().NoError(c.Start(context.Background()))
	s.T().Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.NoError(c.Stop(ctx))
	})
	return c
}

// insertRunning inserts a running row whose deadline is deadlineIn from now.
func (s *RescuerSuite) insertRunning(kind notasap.Kind, attempt, maxAttempts int, deadlineIn time.Duration) int64 {
	s.T().Helper()
	var id int64
	s.Require().NoError(s.Pool.QueryRow(context.Background(), `
INSERT INTO notasap_jobs (kind, status, payload, attempt, max_attempts, lease, deadline)
VALUES ($1, 2, '{}', $2, $3, 1, NOW() + make_interval(secs => $4)) RETURNING id`,
		int16(kind), attempt, maxAttempts, deadlineIn.Seconds()).Scan(&id))
	return id
}

func (s *RescuerSuite) waitStatus(id int64, st notasap.Status) pgtest.JobRow {
	s.T().Helper()
	s.Require().Eventually(func() bool { return s.Job(id).Status == int16(st) }, waitFor, tick)
	return s.Job(id)
}

func (s *RescuerSuite) TestRescuePending() {
	obs := newRescueObserver()
	s.startClient(obs, reg(noop[payloadA], notasap.KindConfig{Name: "a", Backoff: notasap.Constant(time.Hour)}))
	id := s.insertRunning(kindA, 1, 3, -time.Minute)

	row := s.waitStatus(id, notasap.StatusPending)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)
	s.Equal(1, row.Attempt)
	s.Nil(row.Deadline)
	s.WithinDuration(time.Now().Add(time.Hour), row.RunAt, time.Minute)
	s.Equal(int64(1), row.Lease)
	s.Equal(1, obs.Rescued("a"))
}

func (s *RescuerSuite) TestRescueFailed() {
	obs := newRescueObserver()
	s.startClient(obs, reg(noop[payloadA], notasap.KindConfig{Name: "a"}))
	id := s.insertRunning(kindA, 3, 3, -time.Minute)

	row := s.waitStatus(id, notasap.StatusFailed)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)
	s.Equal(3, row.Attempt)
	s.Nil(row.Deadline)
	s.Equal(1, obs.Rescued("a"))
}

func (s *RescuerSuite) TestRescueIgnoresLiveDeadline() {
	obs := newRescueObserver()
	s.startClient(obs, reg(noop[payloadA], notasap.KindConfig{Name: "a"}))
	id := s.insertRunning(kindA, 1, 3, time.Hour)
	before := s.Job(id)

	s.Never(func() bool {
		row := s.Job(id)
		return row.Status != before.Status || !row.UpdatedAt.Equal(before.UpdatedAt) || row.Error != nil
	}, 200*time.Millisecond, tick)
	s.Equal(0, obs.Rescued("a"))
}

func (s *RescuerSuite) TestRescueUnknownKind() {
	obs := newRescueObserver()
	s.startClient(obs, reg(noop[payloadA], notasap.KindConfig{Name: "a", Backoff: notasap.Constant(time.Hour)}))
	id := s.insertRunning(99, 1, 3, -time.Minute)

	row := s.waitStatus(id, notasap.StatusPending)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)
	s.WithinDuration(time.Now().Add(2*time.Hour), row.RunAt, time.Minute)
	s.Equal(1, obs.Rescued("kind_99"))
}

func (s *RescuerSuite) TestStaleWorkerFenced() {
	obs := newRescueObserver()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := s.startClient(obs, reg(func(context.Context, *notasap.Job[payloadA]) error {
		started <- struct{}{}
		<-release
		return nil
	}, notasap.KindConfig{
		Name: "a", Workers: 1, Timeout: 20 * time.Millisecond, MaxAttempts: 3,
		Backoff: notasap.Constant(time.Hour),
	}))
	unblock := sync.OnceFunc(func() { close(release) })
	s.T().Cleanup(unblock) // runs before the client's Stop
	id, err := c.Schedule(context.Background(), payloadA{})
	s.Require().NoError(err)
	<-started

	row := s.waitStatus(id, notasap.StatusPending)
	s.Equal(1, row.Attempt)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)

	unblock()
	s.Never(func() bool { return s.Job(id).Status == int16(notasap.StatusDone) }, 200*time.Millisecond, tick)
	s.Equal(0, obs.Finished(notasap.StatusDone))
	s.Equal(1, obs.Rescued("a"))
}
