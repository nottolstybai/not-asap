package migrations_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/migrations"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

type MigrationsSuite struct {
	pgtest.Suite
}

func TestMigrationsSuite(t *testing.T) {
	suite.Run(t, new(MigrationsSuite))
}

// gooseSections returns the Up and Down halves of the goose migration.
func gooseSections(s *MigrationsSuite) (up, down string) {
	raw, err := migrations.Goose.ReadFile("goose/00001_notasap_jobs.sql")
	s.Require().NoError(err)
	text := string(raw)
	upAt := strings.Index(text, "-- +goose Up")
	downAt := strings.Index(text, "-- +goose Down")
	s.Require().True(upAt >= 0 && downAt > upAt)
	return text[upAt+len("-- +goose Up") : downAt], text[downAt+len("-- +goose Down"):]
}

// newSchema creates a schema named after the suite's random schema name.
func (s *MigrationsSuite) newSchema(prefix string) string {
	name := s.Schema + "_" + prefix
	s.Exec("CREATE SCHEMA " + name)
	s.T().Cleanup(func() { s.Exec("DROP SCHEMA IF EXISTS " + name + " CASCADE") })
	return name
}

func (s *MigrationsSuite) schemaShape(schema string) (columns, indexes []string) {
	ctx := context.Background()
	rows, err := s.Pool.Query(ctx, `
SELECT column_name || '|' || data_type || '|' || is_nullable || '|' || COALESCE(column_default, '')
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = 'notasap_jobs'
ORDER BY ordinal_position`, schema)
	s.Require().NoError(err)
	for rows.Next() {
		var c string
		s.Require().NoError(rows.Scan(&c))
		columns = append(columns, c)
	}
	s.Require().NoError(rows.Err())
	rows.Close()

	rows, err = s.Pool.Query(ctx, `
SELECT indexdef FROM pg_indexes
WHERE schemaname = $1 AND tablename = 'notasap_jobs'
ORDER BY indexname`, schema)
	s.Require().NoError(err)
	for rows.Next() {
		var d string
		s.Require().NoError(rows.Scan(&d))
		indexes = append(indexes, strings.ReplaceAll(d, schema, "S"))
	}
	s.Require().NoError(rows.Err())
	rows.Close()
	return columns, indexes
}

func (s *MigrationsSuite) apply(schema, sql string) {
	pool := pgtest.NewPool(s.T(), schema)
	_, err := pool.Exec(context.Background(), sql)
	s.Require().NoError(err)
}

func (s *MigrationsSuite) TestGooseAndGolangMigrateProduceSameSchema() {
	g, m := s.newSchema("g"), s.newSchema("m")

	gooseUp, _ := gooseSections(s)
	s.apply(g, gooseUp)
	mig, err := migrations.GolangMigrate.ReadFile("golang-migrate/000001_notasap_jobs.up.sql")
	s.Require().NoError(err)
	s.apply(m, string(mig))

	gc, gi := s.schemaShape(g)
	mc, mi := s.schemaShape(m)
	s.Require().NotEmpty(gc)
	s.Require().Equal(gc, mc)
	s.Require().Equal(gi, mi)
	s.Require().Len(gi, 5)
}

func (s *MigrationsSuite) TestDownDropsTable() {
	g, m := s.newSchema("g"), s.newSchema("m")

	up, err := migrations.GolangMigrate.ReadFile("golang-migrate/000001_notasap_jobs.up.sql")
	s.Require().NoError(err)
	down, err := migrations.GolangMigrate.ReadFile("golang-migrate/000001_notasap_jobs.down.sql")
	s.Require().NoError(err)

	gooseUp, gooseDown := gooseSections(s)
	s.apply(g, gooseUp)
	s.apply(g, gooseDown)
	s.apply(m, string(up))
	s.apply(m, string(down))

	for _, schema := range []string{g, m} {
		var reg *string
		s.Require().NoError(s.Pool.QueryRow(context.Background(),
			"SELECT to_regclass($1)::text", schema+".notasap_jobs").Scan(&reg))
		s.Require().Nil(reg, schema)
	}
}
