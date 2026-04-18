//go:build integration

package queue_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/testhelpers"
)

// queryCounter implements pgx.QueryTracer for counting DELETE queries.
type queryCounter struct {
	mu          sync.Mutex
	deleteCount int64
}

func (qc *queryCounter) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := strings.TrimSpace(data.SQL)
	if len(sql) >= 6 && strings.EqualFold(sql[:6], "delete") {
		qc.mu.Lock()
		qc.deleteCount++
		qc.mu.Unlock()
	}
	return ctx
}

func (qc *queryCounter) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
}

func (qc *queryCounter) DeleteCount() int64 {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return qc.deleteCount
}

type JanitorSuite struct {
	testhelpers.DBSuite
}

func (s *JanitorSuite) TestCleansTerminalJobs() {
	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 1
		c.Janitor.Disabled = false
		c.Janitor.TTL = 1 * time.Second
		c.Janitor.Statuses = []notasap.Status{notasap.StatusDone, notasap.StatusExhausted, notasap.StatusCancelled}
		c.Janitor.RunInterval = 100 * time.Millisecond
	})
	s.Require().NoError(err)

	q := notasap.NewQueue(s.Pool, cfg)

	ctx := context.Background()
	for i := range 50 {
		status := notasap.StatusDone
		if i%3 == 1 {
			status = notasap.StatusExhausted
		} else if i%3 == 2 {
			status = notasap.StatusCancelled
		}
		_, err := s.Pool.Exec(ctx, `
			INSERT INTO notasap_jobs (job_type, payload, status, run_after, attempt, max_attempts, created_at, updated_at)
			VALUES ('test_job', '{}', $1, NOW(), 1, 3, NOW() - interval '2 seconds', NOW() - interval '2 seconds')
		`, string(status))
		s.Require().NoError(err)
	}

	q.Start()
	time.Sleep(2 * time.Second)

	var count int64
	err = s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM notasap_jobs
		WHERE status IN ('done', 'exhausted', 'cancelled')
	`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(int64(0), count, "all terminal jobs should be deleted")

	q.Stop(ctx)
}

func (s *JanitorSuite) TestDisabledLeavesTerminalJobs() {
	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 1
		c.Janitor.Disabled = true
	})
	s.Require().NoError(err)

	q := notasap.NewQueue(s.Pool, cfg)

	ctx := context.Background()
	for range 10 {
		_, err := s.Pool.Exec(ctx, `
			INSERT INTO notasap_jobs (job_type, payload, status, run_after, attempt, max_attempts, created_at, updated_at)
			VALUES ('test_job', '{}', 'done', NOW(), 1, 3, NOW() - interval '1 hour', NOW() - interval '1 hour')
		`)
		s.Require().NoError(err)
	}

	q.Start()
	time.Sleep(2 * time.Second)

	var count int64
	err = s.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM notasap_jobs WHERE status = 'done'`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(int64(10), count, "terminal jobs should NOT be deleted when janitor disabled")

	q.Stop(ctx)
}

func TestJanitorSuite(t *testing.T) {
	suite.Run(t, new(JanitorSuite))
}

// TestJanitorBatching_CleansInChunksOf1000 needs a dedicated pool with a query tracer,
// so it runs outside the suite and connects directly to the same test DSN.
func TestJanitorBatching_CleansInChunksOf1000(t *testing.T) {
	testhelpers.SkipIfNoDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pcfg, err := pgxpool.ParseConfig(testhelpers.DSN())
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	counter := &queryCounter{}
	pcfg.ConnConfig.Tracer = counter

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, "TRUNCATE notasap_jobs"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 1
		c.Janitor.Disabled = false
		c.Janitor.TTL = 0
		c.Janitor.Statuses = []notasap.Status{notasap.StatusDone}
		c.Janitor.RunInterval = 100 * time.Millisecond
	})
	if err != nil {
		t.Fatalf("queue config: %v", err)
	}

	q := notasap.NewQueue(pool, cfg)

	for range 2500 {
		_, err := pool.Exec(ctx, `
			INSERT INTO notasap_jobs (job_type, payload, status, run_after, attempt, max_attempts, created_at, updated_at)
			VALUES ('test_job', '{}', 'done', NOW(), 1, 3, NOW(), NOW())
		`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	q.Start()
	time.Sleep(3 * time.Second)

	if got := counter.DeleteCount(); got < 3 {
		t.Fatalf("expected >= 3 DELETE calls for 2500 rows, got %d", got)
	}

	var count int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM notasap_jobs WHERE status = 'done'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 done jobs remaining, got %d", count)
	}

	q.Stop(ctx)
}
