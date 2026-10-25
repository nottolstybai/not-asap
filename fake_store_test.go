package notasap

import (
	"context"
	"sync"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

// finishCall is a recorded Finish or Reschedule call.
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

	mu         sync.Mutex
	claimErr   error
	claimFails int // number of upcoming Claim calls that return claimErr
	claims     int
	rescues    int
	deletes    int
	maxLimit   int
	calls      []finishCall
	writes     []writeResult // scripted results of upcoming Finish/Reschedule calls
}

// writeResult is a scripted result of a status write.
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
			ID: int64(i + 1), Kind: 1, Payload: []byte(`{}`), Attempt: 1, MaxAttempts: 3, Lease: 1,
		}
	}
}

func (f *fakeStore) failClaims(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimFails, f.claimErr = n, err
}

// scriptWrites makes the next Finish/Reschedule calls return rs in order;
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

func (f *fakeStore) Calls() []finishCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]finishCall(nil), f.calls...)
}

func (f *fakeStore) Insert(context.Context, store.DB, []store.InsertParams) ([]store.InsertResult, error) {
	return nil, nil
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

func (f *fakeStore) Finish(_ context.Context, ref store.Ref, status int16, errMsg string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, finishCall{ref: ref, status: Status(status), errMsg: errMsg})
	return f.nextWrite()
}

func (f *fakeStore) Reschedule(_ context.Context, ref store.Ref, delay time.Duration, errMsg string, refund bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, finishCall{ref: ref, status: StatusPending, errMsg: errMsg, delay: delay, refund: refund})
	return f.nextWrite()
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
