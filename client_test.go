package notasap

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

const (
	kindA Kind = 1
	kindB Kind = 2
)

type payloadA struct {
	Msg string `json:"msg"`
	N   int    `json:"n"`
}

func (payloadA) Kind() Kind { return kindA }

type payloadB struct {
	Msg string `json:"msg"`
}

func (payloadB) Kind() Kind { return kindB }

type unknownPayload struct{}

func (unknownPayload) Kind() Kind { return 99 }

type clientSuite struct {
	testSuite
}

func TestClient(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(clientSuite))
}

func (s *clientSuite) TestClient_Start_WithoutTable() {
	schema := "empty_" + strings.ToLower(strings.ReplaceAll(s.Schema, "nt_", ""))
	s.Exec("CREATE SCHEMA " + schema)
	s.ExecOnCleanup("DROP SCHEMA " + schema + " CASCADE")

	c, err := New(pgtest.NewPool(s.T(), schema), WithLogger(slog.New(slog.DiscardHandler)))
	s.Require().NoError(err)
	err = c.Start(s.ctx)
	s.ErrorIs(err, store.ErrNoTable)
	s.NoError(c.Stop(s.ctx))
}

func (s *clientSuite) TestClient_Stop() {
	s.Run("drains running handler", func() {
		started := make(chan struct{}, 1)
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			started <- struct{}{}
			time.Sleep(100 * time.Millisecond) // simulated work that Stop must wait for
			return nil
		}, KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		s.stop(c)
		s.Equal(StatusDone, Status(s.GetJob(id).Status))
	})

	s.Run("timeout refunds attempt", func() {
		started := make(chan struct{}, 1)
		c := s.startClient([]kindReg{reg(func(ctx context.Context, _ *Job[payloadA]) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		ctx, cancel := context.WithTimeout(s.ctx, 50*time.Millisecond)
		defer cancel()
		s.ErrorIs(c.Stop(ctx), context.DeadlineExceeded)

		row := s.GetJob(id)
		s.Equal(StatusPending, Status(row.Status))
		s.Equal(0, row.Attempt)
		s.False(row.RunAt.After(s.DBNow()))
		s.Equal(1, c.obs.Count("retried/shutdown"))
	})
}
