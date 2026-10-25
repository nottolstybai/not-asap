package notasap

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type cancelSuite struct {
	testSuite
}

func TestCancel(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(cancelSuite))
}

func (s *cancelSuite) TestClient_Cancel() {
	c := s.newClient([]kindReg{reg(noop[payloadA], KindConfig{Name: "a"})})

	tests := []struct {
		name       string
		status     Status
		wantErr    error
		wantStatus Status
	}{
		{"pending", StatusPending, nil, StatusCancelled},
		{"running", StatusRunning, ErrJobRunning, StatusRunning},
		{"done", StatusDone, ErrJobFinished, StatusDone},
		{"failed", StatusFailed, ErrJobFinished, StatusFailed},
		{"cancelled", StatusCancelled, ErrJobFinished, StatusCancelled},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
				Kind:   lo.ToPtr(int16(kindA)),
				Status: lo.ToPtr(int16(tt.status)),
			}).ID

			err := c.Cancel(s.ctx, id)
			if tt.wantErr == nil {
				s.NoError(err)
			} else {
				s.ErrorIs(err, tt.wantErr)
			}
			s.Equal(tt.wantStatus, Status(s.GetJob(id).Status))
		})
	}

	s.Run("not found", func() {
		s.ErrorIs(c.Cancel(s.ctx, -1), ErrNotFound)
	})
}
