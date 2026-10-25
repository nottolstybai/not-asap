package store_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type cancelSuite struct {
	testSuite
}

func TestCancel(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(cancelSuite))
}

func (s *cancelSuite) TestStore_Cancel() {
	tests := []struct {
		name   string
		status int16
		want   store.CancelOutcome
	}{
		{"pending", store.StatusPending, store.CancelOK},
		{"running", store.StatusRunning, store.CancelRunning},
		{"done", store.StatusDone, store.CancelFinished},
		{"failed", store.StatusFailed, store.CancelFinished},
		{"cancelled", store.StatusCancelled, store.CancelFinished},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			id := s.Fixture.Job(s.ctx, pgtest.JobOpt{Status: new(tt.status)}).ID
			got, err := s.st.Cancel(s.ctx, id)
			s.Require().NoError(err)
			s.Equal(tt.want, got)
		})
	}
	s.Run("not found", func() {
		got, err := s.st.Cancel(s.ctx, -1)
		s.Require().NoError(err)
		s.Equal(store.CancelNotFound, got)
	})
}
