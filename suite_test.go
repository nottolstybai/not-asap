package notasap

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/nottolstybai/not-asap/internal/pgtest"
)

// testSuite is the base of the integration suites of this package.
type testSuite struct {
	pgtest.Suite
	ctx context.Context
}

func (s *testSuite) SetupTest() {
	s.Suite.SetupTest()
	s.ctx = s.T().Context()
}

// kindReg registers one kind on a client.
type kindReg func(c *Client) error

func reg[T Payload](h Handler[T], cfg KindConfig) kindReg {
	return func(c *Client) error { return Register(c, h, cfg) }
}

// recorder is an Observer that counts events by key: "scheduled/<kind>",
// "finished/<status>", "retried/<reason>", "stuck/<kind>", "rescued/<kind>",
// "polled/<kind>";
// janitor batch sizes are kept in order.
type recorder struct {
	NopObserver
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
func (r *recorder) JobFinished(_ string, st Status, _ time.Duration) {
	r.inc("finished/" + st.String())
}
func (r *recorder) JobRetried(_ string, reason RetryReason, _ time.Duration) {
	r.inc("retried/" + string(reason))
}
func (r *recorder) JobStuck(kind string)                { r.inc("stuck/" + kind) }
func (r *recorder) JobRescued(kind string)              { r.inc("rescued/" + kind) }
func (r *recorder) Polled(kind string, _ time.Duration) { r.inc("polled/" + kind) }
func (r *recorder) JanitorDeleted(_ Status, n int64) {
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
	*Client
	obs *recorder
}

// newClient returns an unstarted client over the suite pool with the given
// kinds. It logs nowhere, reports to a fresh recorder, polls every 10ms with
// no retry backoff, and runs maintenance hourly so tests drive it through
// rescue/clean. opts are applied over these defaults.
func (s *testSuite) newClient(kinds []kindReg, opts ...Option) *testClient {
	s.T().Helper()
	return s.newClientOn(s.Pool, kinds, opts...)
}

// newClientOn is newClient over pool.
func (s *testSuite) newClientOn(pool *pgxpool.Pool, kinds []kindReg, opts ...Option) *testClient {
	s.T().Helper()
	obs := &recorder{}
	c, err := New(pool, append([]Option{
		WithLogger(slog.New(slog.DiscardHandler)),
		WithObserver(obs),
		WithDefaults(KindConfig{
			PollInterval: 10 * time.Millisecond,
			Backoff:      Constant(0),
		}),
		WithRescuer(RescuerConfig{Interval: time.Hour}),
		WithJanitor(JanitorConfig{Interval: time.Hour}),
	}, opts...)...)
	s.Require().NoError(err)
	for _, k := range kinds {
		s.Require().NoError(k(c))
	}
	return &testClient{
		Client: c,
		obs:    obs,
	}
}

// startClient is newClient, started and stopped on test cleanup. Cleanups
// registered later by the test run before that Stop.
func (s *testSuite) startClient(kinds []kindReg, opts ...Option) *testClient {
	s.T().Helper()
	return s.startClientOn(s.Pool, kinds, opts...)
}

// startClientOn is startClient over pool.
func (s *testSuite) startClientOn(pool *pgxpool.Pool, kinds []kindReg, opts ...Option) *testClient {
	s.T().Helper()
	c := s.newClientOn(pool, kinds, opts...)
	s.Require().NoError(c.Start(s.ctx))
	// Cleanups run after s.T() may have moved on (e.g. inside s.Run), so
	// report on the T that registered it. s.ctx is cancelled by then.
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
func (s *testSuite) stop(c *testClient) {
	s.T().Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	s.Require().NoError(c.Stop(ctx))
}

func (s *testSuite) schedule(c *testClient, p Payload, opts ...ScheduleOption) int64 {
	s.T().Helper()
	id, err := c.Schedule(s.ctx, p, opts...)
	s.Require().NoError(err)
	return id
}
