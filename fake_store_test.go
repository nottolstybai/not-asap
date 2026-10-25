package notasap

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

// finishCall is one row of a recorded Complete call.
type finishCall struct {
	ref    store.Ref
	status Status
	errMsg string
	delay  time.Duration
	refund bool
}

// fakeStore is an in-memory jobStore for pool tests. Jobs fed into pending
// are returned by Claim; status writes are recorded and always succeed.
type fakeStore struct {
	pending chan store.ClaimedJob
	// claimHook, if set before Start, runs at the start of every Claim; a
	// non-nil error is returned from Claim.
	claimHook func(ctx context.Context) error
	// deleteHook, if set before Start, runs at the start of every
	// DeleteFinished; a non-nil error is returned from it.
	deleteHook func(ctx context.Context) error
	// completeHook, if set before Start, runs at the start of every
	// Complete.
	completeHook func(ctx context.Context)

	mu         sync.Mutex
	claimErr   error
	claimFails int // number of upcoming Claim calls that return claimErr
	claims     int
	rescues    int
	deletes    int
	maxLimit   int
	limits     []int // limit of every Claim call, in order
	calls      []finishCall
	batches    []int         // rows of every Complete call, in order
	writes     []writeResult // scripted results of upcoming Complete calls
}

// writeResult is a scripted result of a Complete call; ok == false means
// no row was written.
type writeResult struct {
	ok  bool
	err error
}

var _ jobStore = (*fakeStore)(nil)

func newFakeStore() *fakeStore {
	return &fakeStore{pending: make(chan store.ClaimedJob, 1000)}
}

// feed queues n jobs of kind 1 with ids starting at 1.
func (f *fakeStore) feed(n int) {
	for i := range n {
		f.pending <- store.ClaimedJob{
			ID:          int64(i + 1),
			Kind:        1,
			Payload:     []byte(`{}`),
			Attempt:     1,
			MaxAttempts: 3,
			Lease:       1,
		}
	}
}

func (f *fakeStore) failClaims(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimFails, f.claimErr = n, err
}

// scriptWrites makes the next Complete calls return rs in order;
// later calls succeed.
func (f *fakeStore) scriptWrites(rs ...writeResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = rs
}

// nextWrite pops the next scripted result; f.mu must be held.
func (f *fakeStore) nextWrite() (bool, error) {
	if len(f.writes) == 0 {
		return true, nil
	}
	r := f.writes[0]
	f.writes = f.writes[1:]
	return r.ok, r.err
}

func (f *fakeStore) Claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

func (f *fakeStore) Rescues() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rescues
}

func (f *fakeStore) Deletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

func (f *fakeStore) MaxLimit() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxLimit
}

func (f *fakeStore) Limits() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.limits)
}

func (f *fakeStore) Calls() []finishCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]finishCall(nil), f.calls...)
}

// Insert reports every job as inserted; it does not queue them for Claim.
// Batches returns the number of rows of every Complete call.
func (f *fakeStore) Batches() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.batches)
}

func (f *fakeStore) Insert(_ context.Context, _ store.DB, params []store.InsertParams) ([]store.InsertResult, error) {
	res := make([]store.InsertResult, len(params))
	for i := range res {
		res[i] = store.InsertResult{
			ID:      int64(i + 1),
			Outcome: store.Inserted,
		}
	}
	return res, nil
}

func (f *fakeStore) Claim(ctx context.Context, _ int16, limit int, _ time.Duration) ([]store.ClaimedJob, error) {
	if f.claimHook != nil {
		if err := f.claimHook(ctx); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	f.claims++
	f.maxLimit = max(f.maxLimit, limit)
	f.limits = append(f.limits, limit)
	if f.claimFails > 0 {
		f.claimFails--
		err := f.claimErr
		f.mu.Unlock()
		return nil, err
	}
	f.mu.Unlock()

	var jobs []store.ClaimedJob
	for len(jobs) < limit {
		select {
		case j := <-f.pending:
			jobs = append(jobs, j)
		default:
			return jobs, nil
		}
	}
	return jobs, nil
}

func (f *fakeStore) Complete(ctx context.Context, cs []store.Completion) ([]store.Ref, error) {
	if f.completeHook != nil {
		f.completeHook(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, len(cs))
	refs := make([]store.Ref, len(cs))
	for i, cp := range cs {
		f.calls = append(f.calls, finishCall{
			ref:    cp.Ref,
			status: Status(cp.Status),
			errMsg: cp.Error,
			delay:  cp.Delay,
			refund: cp.Refund,
		})
		refs[i] = cp.Ref
	}
	ok, err := f.nextWrite()
	if err != nil || !ok {
		return nil, err
	}
	return refs, nil
}

func (f *fakeStore) Cancel(context.Context, int64) (store.CancelOutcome, error) {
	return 0, nil
}

func (f *fakeStore) Rescue(context.Context, int, func(store.StuckJob) store.RescueDecision) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rescues++
	return 0, nil
}

func (f *fakeStore) DeleteFinished(ctx context.Context, _ int16, _ time.Duration, _ int) (int64, error) {
	f.mu.Lock()
	f.deletes++
	f.mu.Unlock()
	if f.deleteHook != nil {
		if err := f.deleteHook(ctx); err != nil {
			return 0, err
		}
	}
	return 0, nil
}

func (f *fakeStore) CheckSchema(context.Context) error { return nil }
