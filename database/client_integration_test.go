//go:build integration

package database_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
	"github.com/nottolstybai/not-asap/internal/testhelpers"
)

type clientTestJob struct {
	Message string `json:"message"`
}

func (clientTestJob) Kind() string { return "test" }

type ClientSuite struct {
	testhelpers.DBSuite
	client *database.Client
}

func (s *ClientSuite) SetupTest() {
	s.DBSuite.SetupTest()
	s.client = database.NewClient(s.Pool)
}

func (s *ClientSuite) TestEnqueue() {
	ctx := context.Background()
	job, err := s.client.Enqueue(ctx, "test", clientTestJob{Message: "hello"}, 3)
	s.Require().NoError(err)
	s.NotEmpty(job.ID)
	s.Equal("test", job.JobType)
	s.Equal(core.StatusNew, job.Status)
	s.Equal(3, job.MaxAttempts)

	var status string
	var maxAttempts int
	var payload []byte
	err = s.Pool.QueryRow(ctx,
		"SELECT status, max_attempts, payload FROM notasap_jobs WHERE id = $1", job.ID,
	).Scan(&status, &maxAttempts, &payload)
	s.Require().NoError(err)
	s.Equal("new", status)
	s.Equal(3, maxAttempts)

	var decoded clientTestJob
	s.Require().NoError(json.Unmarshal(payload, &decoded))
	s.Equal("hello", decoded.Message)
}

func (s *ClientSuite) TestEnqueueBatch() {
	ctx := context.Background()
	jobs := make([]database.BatchJob, 10)
	for i := range 10 {
		jobs[i] = database.BatchJob{Kind: "test", Args: clientTestJob{Message: "batch"}, MaxAttempts: 3}
	}
	s.Require().NoError(s.client.EnqueueBatch(ctx, jobs))

	var count int
	err := s.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM notasap_jobs WHERE job_type='test' AND status='new'",
	).Scan(&count)
	s.Require().NoError(err)
	s.Equal(10, count)
}

func (s *ClientSuite) TestEnqueueTxCommit() {
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)

	job, err := s.client.EnqueueTx(ctx, tx, "test", clientTestJob{Message: "tx-job"}, 3)
	s.Require().NoError(err)
	s.NotEmpty(job.ID)

	s.Require().NoError(tx.Commit(ctx))

	var status string
	err = s.Pool.QueryRow(ctx,
		"SELECT status FROM notasap_jobs WHERE id = $1", job.ID,
	).Scan(&status)
	s.Require().NoError(err)
	s.Equal("new", status)
}

func (s *ClientSuite) TestEnqueueTxRollback() {
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)

	_, err = s.client.EnqueueTx(ctx, tx, "test", clientTestJob{Message: "rollback-job"}, 3)
	s.Require().NoError(err)

	s.Require().NoError(tx.Rollback(ctx))

	var count int
	err = s.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM notasap_jobs WHERE job_type='test'",
	).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count, "row must be absent after rollback")
}

func (s *ClientSuite) TestEnqueueBatchTxCommit() {
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)

	jobs := make([]database.BatchJob, 5)
	for i := range 5 {
		jobs[i] = database.BatchJob{Kind: "test", Args: clientTestJob{Message: "batch-tx"}, MaxAttempts: 3}
	}
	s.Require().NoError(s.client.EnqueueBatchTx(ctx, tx, jobs))
	s.Require().NoError(tx.Commit(ctx))

	var count int
	err = s.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM notasap_jobs WHERE job_type='test'",
	).Scan(&count)
	s.Require().NoError(err)
	s.Equal(5, count)
}

func (s *ClientSuite) TestEnqueueBatchTxRollback() {
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)

	jobs := make([]database.BatchJob, 3)
	for i := range 3 {
		jobs[i] = database.BatchJob{Kind: "test", Args: clientTestJob{Message: "batch-rollback"}, MaxAttempts: 3}
	}
	s.Require().NoError(s.client.EnqueueBatchTx(ctx, tx, jobs))
	s.Require().NoError(tx.Rollback(ctx))

	var count int
	err = s.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM notasap_jobs WHERE job_type='test'",
	).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count, "all rows must be absent after rollback")
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}
