package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

// testSuite is the base of the store suites.
type testSuite struct {
	pgtest.Suite
	ctx context.Context
	st  *store.Store
}

func (s *testSuite) SetupSuite() {
	s.Suite.SetupSuite()
	s.st = store.New(s.Pool)
}

func (s *testSuite) SetupTest() {
	s.Suite.SetupTest()
	s.ctx = s.T().Context()
}

type storeSuite struct {
	testSuite
}

func TestStore(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(storeSuite))
}

func (s *storeSuite) TestStore_CheckSchema() {
	s.NoError(s.st.CheckSchema(s.ctx))

	schema := "nt_empty_" + s.Schema
	s.Exec("CREATE SCHEMA " + schema)
	s.ExecOnCleanup("DROP SCHEMA " + schema + " CASCADE")
	pool := pgtest.NewPool(s.T(), schema)

	err := store.New(pool).CheckSchema(s.ctx)
	s.ErrorIs(err, store.ErrNoTable)
}
