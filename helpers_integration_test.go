package notasap_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

const (
	waitFor = pgtest.WaitTimeout
	tick    = pgtest.Tick
)

func noop[T notasap.Payload](context.Context, *notasap.Job[T]) error { return nil }

// kindReg registers one kind on a client.
type kindReg func(c *notasap.Client) error

func reg[T notasap.Payload](h notasap.Handler[T], cfg notasap.KindConfig) kindReg {
	return func(c *notasap.Client) error { return notasap.Register(c, h, cfg) }
}

// recorder is an Observer that counts events by key: "scheduled/<kind>",
// "finished/<status>", "retried/<reason>", "stuck/<kind>", "rescued/<kind>",
// "polled/<kind>";
// janitor batch sizes are kept in order.
type recorder struct {
	notasap.NopObserver
	mu      sync.Mutex
	counts  map[string]int
	batches []int64
}

func (r *recorder) inc(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = make(map[string]int)
	}
	r.counts[key]++
}

func (r *recorder) JobScheduled(kind string) { r.inc("scheduled/" + kind) }
func (r *recorder) JobFinished(_ string, st notasap.Status, _ time.Duration) {
	r.inc("finished/" + st.String())
}
func (r *recorder) JobRetried(_ string, reason notasap.RetryReason, _ time.Duration) {
	r.inc("retried/" + string(reason))
}
func (r *recorder) JobStuck(kind string)                { r.inc("stuck/" + kind) }
func (r *recorder) JobRescued(kind string)              { r.inc("rescued/" + kind) }
func (r *recorder) Polled(kind string, _ time.Duration) { r.inc("polled/" + kind) }
func (r *recorder) JanitorDeleted(_ notasap.Status, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, n)
}

// Count returns how many events with key were recorded.
func (r *recorder) Count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[key]
}

// Batches returns the janitor batch sizes recorded so far.
func (r *recorder) Batches() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.batches...)
}

// testClient is a client with the recorder it reports to.
type testClient struct {
	*notasap.Client
	obs *recorder
}

// clientSuite is the base of the root integration suites.
type clientSuite struct {
	pgtest.Suite
}

// newClient returns an unstarted client over the suite pool with the given
// kinds. It logs nowhere, reports to a fresh recorder, polls every 10ms with
// no retry backoff, and runs maintenance hourly so tests drive it through
// RescueNow/CleanNow. opts are applied over these defaults.
func (s *clientSuite) newClient(kinds []kindReg, opts ...notasap.Option) *testClient {
	s.T().Helper()
	return s.newClientOn(s.Pool, kinds, opts...)
}

// newClientOn is newClient over pool.
func (s *clientSuite) newClientOn(pool *pgxpool.Pool, kinds []kindReg, opts ...notasap.Option) *testClient {
	s.T().Helper()
	obs := &recorder{}
	c, err := notasap.New(pool, append([]notasap.Option{
		notasap.WithLogger(slog.New(slog.DiscardHandler)),
		notasap.WithObserver(obs),
		notasap.WithDefaults(notasap.KindConfig{PollInterval: 10 * time.Millisecond, Backoff: notasap.Constant(0)}),
		notasap.WithRescuer(notasap.RescuerConfig{Interval: time.Hour}),
		notasap.WithJanitor(notasap.JanitorConfig{Interval: time.Hour}),
	}, opts...)...)
	s.Require().NoError(err)
	for _, k := range kinds {
		s.Require().NoError(k(c))
	}
	return &testClient{Client: c, obs: obs}
}

// startClient is newClient, started and stopped on test cleanup. Cleanups
// registered later by the test run before that Stop.
func (s *clientSuite) startClient(kinds []kindReg, opts ...notasap.Option) *testClient {
	s.T().Helper()
	return s.startClientOn(s.Pool, kinds, opts...)
}

// startClientOn is startClient over pool.
func (s *clientSuite) startClientOn(pool *pgxpool.Pool, kinds []kindReg, opts ...notasap.Option) *testClient {
	s.T().Helper()
	c := s.newClientOn(pool, kinds, opts...)
	s.Require().NoError(c.Start(context.Background()))
	// Cleanups run after s.T() may have moved on (e.g. inside s.Run), so
	// report on the T that registered it.
	t := s.T()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, c.Stop(ctx))
	})
	return c
}

// stop stops c and requires success. Stop drains running handlers including
// their status writes and observer calls, so it is a barrier for asserts.
func (s *clientSuite) stop(c *testClient) {
	s.T().Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(c.Stop(ctx))
}

func (s *clientSuite) schedule(c *testClient, p notasap.Payload, opts ...notasap.ScheduleOption) int64 {
	s.T().Helper()
	id, err := c.Schedule(context.Background(), p, opts...)
	s.Require().NoError(err)
	return id
}

// status returns the status of job id.
func (s *clientSuite) status(id int64) notasap.Status {
	s.T().Helper()
	return notasap.Status(s.GetJob(id).Status)
}

// waitStatus waits until job id has status st and returns the row.
func (s *clientSuite) waitStatus(id int64, st notasap.Status) pgtest.JobRow {
	s.T().Helper()
	return pgtest.WaitStatus(&s.Suite, id, st)
}

// recv waits for a value on ch or fails the test after waitFor.
func recv[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitFor):
		t.Fatal("timed out waiting on channel")
		panic("unreachable")
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent log writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
