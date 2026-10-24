package notasap_test

import (
	"context"
	"os"
	"sync"
	"testing"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

const (
	kindA notasap.Kind = 1
	kindB notasap.Kind = 2
)

type payloadA struct {
	Msg string `json:"msg"`
	N   int    `json:"n"`
}

func (payloadA) Kind() notasap.Kind { return kindA }

type payloadB struct {
	Msg string `json:"msg"`
}

func (payloadB) Kind() notasap.Kind { return kindB }

type unregisteredPayload struct{}

func (unregisteredPayload) Kind() notasap.Kind { return 99 }

func noop[T notasap.Payload](context.Context, *notasap.Job[T]) error { return nil }

// countingObserver counts JobScheduled calls per kind name.
type countingObserver struct {
	notasap.NopObserver
	mu        sync.Mutex
	scheduled map[string]int
}

func newCountingObserver() *countingObserver {
	return &countingObserver{scheduled: make(map[string]int)}
}

func (o *countingObserver) JobScheduled(kind string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.scheduled[kind]++
}

func (o *countingObserver) Scheduled(kind string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.scheduled[kind]
}

// countJobs returns the number of rows in notasap_jobs.
func countJobs(s *pgtest.Suite) int {
	s.T().Helper()
	var n int
	s.Require().NoError(s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM notasap_jobs").Scan(&n))
	return n
}
