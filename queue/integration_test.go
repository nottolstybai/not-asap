//go:build integration

package queue_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/testhelpers"
)

type snapshotTestJob struct {
	Message string `json:"message"`
}

func (snapshotTestJob) Kind() string { return "test" }

type QueueSuite struct {
	testhelpers.DBSuite
}

func (s *QueueSuite) TestConfigSnapshotImmutable() {
	cfg, err := notasap.NewQueueConfig()
	s.Require().NoError(err)
	q := notasap.NewQueue(s.Pool, cfg)

	notasap.Register(q, func(ctx context.Context, job snapshotTestJob) error {
		return nil
	}, func(tc *notasap.TaskConfig) {
		tc.MaxRetries = 7
	})

	job, err := q.Enqueue(context.Background(), snapshotTestJob{Message: "snapshot"})
	s.Require().NoError(err)
	s.Equal(7, job.MaxAttempts, "max_attempts must be snapshotted from per-type config")

	var maxAttempts int
	err = s.Pool.QueryRow(context.Background(),
		"SELECT max_attempts FROM notasap_jobs WHERE id = $1", job.ID,
	).Scan(&maxAttempts)
	s.Require().NoError(err)
	s.Equal(7, maxAttempts, "DB row must have max_attempts=7")
}

func TestQueueSuite(t *testing.T) {
	suite.Run(t, new(QueueSuite))
}
