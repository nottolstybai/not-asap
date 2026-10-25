package notasap_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type CancelSuite struct {
	clientSuite
}

func TestCancelSuite(t *testing.T) {
	suite.Run(t, new(CancelSuite))
}

func (s *CancelSuite) TestCancel() {
	c := s.newClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a"})})

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
			id := s.InsertJob(pgtest.WithKind(kindA), pgtest.WithStatus(tt.status))

			err := c.Cancel(context.Background(), id)
			if tt.wantErr == nil {
				s.Require().NoError(err)
			} else {
				s.Require().ErrorIs(err, tt.wantErr)
			}
			s.Equal(tt.wantStatus, s.status(id))
		})
	}

	s.Run("not found", func() {
		s.Require().ErrorIs(c.Cancel(context.Background(), -1), notasap.ErrNotFound)
	})
}
