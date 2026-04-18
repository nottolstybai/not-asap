//go:build integration

package testhelpers

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
)

const DefaultDSN = "postgres://notasap:notasap@localhost:5432/notasap_test?sslmode=disable"

func DSN() string {
	if v := os.Getenv("NOTASAP_TEST_DSN"); v != "" {
		return v
	}
	return DefaultDSN
}

// DBSuite is the base suite for integration tests that need a live Postgres.
// Expects `make testdb` (or equivalent) to have created the database and
// applied migrations BEFORE the test run.
//
// Per-test isolation: TRUNCATE notasap_jobs in SetupTest.
type DBSuite struct {
	suite.Suite
	Pool *pgxpool.Pool
}

func (s *DBSuite) SetupSuite() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, DSN())
	s.Require().NoError(err, "connect to %s — did you run `make testdb`?", DSN())
	s.Require().NoError(pool.Ping(ctx), "ping DB — did you run `make testdb`?")

	var exists bool
	err = pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'notasap_jobs'
        )`).Scan(&exists)
	s.Require().NoError(err)
	s.Require().True(exists, "notasap_jobs table missing — run `make migrate-up`")

	s.Pool = pool
}

func (s *DBSuite) TearDownSuite() {
	if s.Pool != nil {
		s.Pool.Close()
	}
}

func (s *DBSuite) SetupTest() {
	_, err := s.Pool.Exec(context.Background(), "TRUNCATE notasap_jobs")
	s.Require().NoError(err)
}

// SkipIfNoDSN lets standalone integration tests gate themselves when the DB is unreachable.
func SkipIfNoDSN(t *testing.T) {
	t.Helper()
	if os.Getenv("NOTASAP_TEST_DSN") == "" && !canConnect(DefaultDSN) {
		t.Skip("integration DSN not reachable — run `make testdb`")
	}
}

func canConnect(dsn string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return false
	}
	defer p.Close()
	return p.Ping(ctx) == nil
}
