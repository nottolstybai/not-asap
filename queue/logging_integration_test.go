//go:build integration

package queue_test

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/testhelpers"
)

type logTestJob struct{}

func (logTestJob) Kind() string { return "logtestjob" }

type injectTestJob struct{}

func (injectTestJob) Kind() string { return "injecttestjob" }

type panicLogTestJob struct{}

func (panicLogTestJob) Kind() string { return "paniclogtestjob" }

type LoggingSuite struct {
	testhelpers.DBSuite
}

func (s *LoggingSuite) TestLifecycleEventsLogged() {
	var buf bytes.Buffer
	customLogger := slog.New(slog.NewTextHandler(&buf, nil))

	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 2
		c.PollInterval = 100 * time.Millisecond
		c.Logger = customLogger
		c.Janitor.RunInterval = time.Hour
	})
	s.Require().NoError(err)

	q := notasap.NewQueue(s.Pool, cfg)

	notasap.Register(q, func(ctx context.Context, args logTestJob) error {
		return nil
	})

	q.Start()
	time.Sleep(200 * time.Millisecond)

	_, err = q.Enqueue(context.Background(), logTestJob{})
	s.Require().NoError(err)

	time.Sleep(1 * time.Second)
	q.Stop(context.Background())

	output := buf.String()
	s.Contains(output, `msg="worker pool started"`)
	s.Contains(output, `msg="worker pool stopped"`)
	s.Contains(output, `msg="job completed"`)
	s.Contains(output, `job_id=`)
	s.Contains(output, `job_type=`)
	s.Contains(output, `msg="janitor started"`)
	s.Contains(output, `msg="janitor stopped"`)
}

func (s *LoggingSuite) TestLoggerInjectionHandlerCanExtract() {
	var buf bytes.Buffer
	customLogger := slog.New(slog.NewTextHandler(&buf, nil))

	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 1
		c.PollInterval = 100 * time.Millisecond
		c.Logger = customLogger
		c.Janitor.RunInterval = time.Hour
	})
	s.Require().NoError(err)

	q := notasap.NewQueue(s.Pool, cfg)

	var handlerCalled atomic.Bool
	notasap.Register(q, func(ctx context.Context, args injectTestJob) error {
		logger := notasap.LoggerFromContext(ctx)
		logger.Info("handler extracted logger")
		handlerCalled.Store(true)
		return nil
	})

	q.Start()

	_, err = q.Enqueue(context.Background(), injectTestJob{})
	s.Require().NoError(err)

	time.Sleep(1 * time.Second)
	q.Stop(context.Background())

	s.True(handlerCalled.Load(), "handler should have been called")
	output := buf.String()
	s.Contains(output, `msg="handler extracted logger"`)
	s.Contains(output, `job_id=`)
	s.Contains(output, `job_type=`)
}

func (s *LoggingSuite) TestPanicWithStackTrace() {
	var buf bytes.Buffer
	customLogger := slog.New(slog.NewTextHandler(&buf, nil))

	cfg, err := notasap.NewQueueConfig(func(c *notasap.QueueConfig) {
		c.WorkerCount = 1
		c.PollInterval = 100 * time.Millisecond
		c.Logger = customLogger
		c.MaxRetries = 1
		c.Janitor.RunInterval = time.Hour
	})
	s.Require().NoError(err)

	q := notasap.NewQueue(s.Pool, cfg)

	notasap.Register(q, func(ctx context.Context, args panicLogTestJob) error {
		panic("test panic for logging verification")
	})

	q.Start()

	_, err = q.Enqueue(context.Background(), panicLogTestJob{})
	s.Require().NoError(err)

	time.Sleep(1 * time.Second)
	q.Stop(context.Background())

	output := buf.String()
	s.Contains(output, `level=ERROR`)
	s.Contains(output, `msg="job panicked"`)
	s.Contains(output, `panic_value`)
	s.Contains(output, "test panic for logging verification")
	s.Contains(output, `stack_trace`)
	s.Contains(output, "goroutine")
}

func TestLoggingSuite(t *testing.T) {
	suite.Run(t, new(LoggingSuite))
}
