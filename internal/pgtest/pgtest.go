// Package pgtest is the integration-test harness: a Postgres container (or an
// external DSN), a per-suite schema with the notasap table, and row helpers.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/nottolstybai/not-asap/migrations"
)

var dsn string

// hasDSN reports whether a database is available.
func hasDSN() bool { return dsn != "" }

// Run is meant to be called from TestMain: os.Exit(pgtest.Run(m)).
// Under -short no database is started.
func Run(m *testing.M) int {
	flag.Parse()
	if testing.Short() {
		return m.Run()
	}
	if env := os.Getenv("NOTASAP_TEST_DSN"); env != "" {
		dsn = env
		return m.Run()
	}

	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("notasap"),
		tcpostgres.WithUsername("notasap"),
		tcpostgres.WithPassword("notasap"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: start postgres container: %v\n", err)
		return 1
	}
	code := func() int {
		d, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: connection string: %v\n", err)
			return 1
		}
		dsn = d
		return m.Run()
	}()
	if err := testcontainers.TerminateContainer(ctr); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: terminate container: %v\n", err)
	}
	return code
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newPool(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if schema != "" {
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// NewPool opens an extra pool whose search_path is schema. It is closed on test cleanup.
func NewPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	pool, err := newPool(context.Background(), schema)
	if err != nil {
		t.Fatalf("pgtest: open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// NewSchema creates and migrates one more schema, returns its name and a pool
// whose search_path is that schema. The schema is dropped on test cleanup.
func NewSchema(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	schema := "nt_" + randHex(6)
	admin, err := newPool(ctx, "")
	if err != nil {
		t.Fatalf("pgtest: open admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("pgtest: create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("pgtest: drop schema: %v", err)
		}
	})
	pool := NewPool(t, schema)
	up, err := migrations.GolangMigrate.ReadFile("golang-migrate/000001_notasap_jobs.up.sql")
	if err != nil {
		t.Fatalf("pgtest: read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(up)); err != nil {
		t.Fatalf("pgtest: migrate: %v", err)
	}
	return schema, pool
}

// JobRow mirrors a notasap_jobs row.
type JobRow struct {
	ID          int64
	Kind        int16
	Status      int16
	Payload     []byte
	Attempt     int
	MaxAttempts int
	RunAt       time.Time
	Deadline    *time.Time
	Lease       int64
	Key         *string
	Error       *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Suite is a testify suite with an isolated schema holding the notasap table.
type Suite struct {
	suite.Suite
	Pool   *pgxpool.Pool
	Schema string
	admin  *pgxpool.Pool
}

func (s *Suite) SetupSuite() {
	if !hasDSN() {
		s.T().Skip("no database (short mode)")
	}
	ctx := context.Background()
	s.Schema = "nt_" + randHex(6)

	admin, err := newPool(ctx, "")
	s.Require().NoError(err)
	s.admin = admin
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+s.Schema)
	s.Require().NoError(err)

	s.Pool, err = newPool(ctx, s.Schema)
	s.Require().NoError(err)

	up, err := migrations.GolangMigrate.ReadFile("golang-migrate/000001_notasap_jobs.up.sql")
	s.Require().NoError(err)
	s.Exec(string(up))
}

func (s *Suite) TearDownSuite() {
	ctx := context.Background()
	if s.admin != nil {
		_, err := s.admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+s.Schema+" CASCADE")
		s.NoError(err)
		s.admin.Close()
	}
	if s.Pool != nil {
		s.Pool.Close()
	}
}

func (s *Suite) SetupTest() {
	s.Exec("TRUNCATE notasap_jobs")
}

// SetupSubTest empties the table before every s.Run subtest too, so
// subtests do not see each other's rows.
func (s *Suite) SetupSubTest() {
	s.Exec("TRUNCATE notasap_jobs")
}

// ExecOnCleanup runs sql on the suite pool when the current test ends and
// reports a failure on that test's T, not on whatever s.T() is by then.
func (s *Suite) ExecOnCleanup(sql string) {
	t := s.T()
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(context.Background(), sql); err != nil {
			t.Errorf("pgtest: cleanup %q: %v", sql, err)
		}
	})
}

// Exec runs SQL on the suite pool and requires success.
func (s *Suite) Exec(sql string, args ...any) {
	s.T().Helper()
	_, err := s.Pool.Exec(context.Background(), sql, args...)
	s.Require().NoError(err)
}

// Job reads one row by id.
func (s *Suite) Job(id int64) JobRow {
	s.T().Helper()
	var r JobRow
	err := s.Pool.QueryRow(context.Background(), `
SELECT id, kind, status, payload, attempt, max_attempts, run_at, deadline, lease,
       idempotency_key, error, created_at, updated_at
FROM notasap_jobs WHERE id = $1`, id).Scan(
		&r.ID, &r.Kind, &r.Status, &r.Payload, &r.Attempt, &r.MaxAttempts, &r.RunAt,
		&r.Deadline, &r.Lease, &r.Key, &r.Error, &r.CreatedAt, &r.UpdatedAt)
	s.Require().NoError(err)
	return r
}
