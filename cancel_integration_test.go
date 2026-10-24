package notasap_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type CancelSuite struct {
	pgtest.Suite
}

func TestCancelSuite(t *testing.T) {
	suite.Run(t, new(CancelSuite))
}

func (s *CancelSuite) TestCancel() {
	c, err := notasap.New(s.Pool)
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c, noop[payloadA], notasap.KindConfig{Name: "a"}))

	tests := []struct {
		name       string
		status     notasap.Status
		wantErr    error
		wantStatus notasap.Status
	}{
		{"pending", notasap.StatusPending, nil, notasap.StatusCancelled},
		{"running", notasap.StatusRunning, notasap.ErrJobRunning, notasap.StatusRunning},
		{"done", notasap.StatusDone, notasap.ErrJobFinished, notasap.StatusDone},
		{"failed", notasap.StatusFailed, notasap.ErrJobFinished, notasap.StatusFailed},
		{"cancelled", notasap.StatusCancelled, notasap.ErrJobFinished, notasap.StatusCancelled},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			id, err := c.Schedule(context.Background(), payloadA{})
			s.Require().NoError(err)
			s.Exec("UPDATE notasap_jobs SET status = $2 WHERE id = $1", id, int16(tt.status))

			err = c.Cancel(context.Background(), id)
			if tt.wantErr == nil {
				s.Require().NoError(err)
			} else {
				s.Require().ErrorIs(err, tt.wantErr)
			}
			s.Equal(int16(tt.wantStatus), s.Job(id).Status)
		})
	}

	s.Run("not found", func() {
		s.Require().ErrorIs(c.Cancel(context.Background(), 999), notasap.ErrNotFound)
	})
}
