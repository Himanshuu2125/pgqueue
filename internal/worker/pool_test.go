package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

type call struct {
	op    string
	msg   string
	delay *time.Duration
}

type fakeStore struct{ calls []call }

func (f *fakeStore) Claim(context.Context, string, []string, int, time.Duration) ([]model.Job, error) {
	return nil, nil
}
func (f *fakeStore) Heartbeat(context.Context, []store.Lease, time.Duration) ([]uuid.UUID, []uuid.UUID, error) {
	return nil, nil, nil
}
func (f *fakeStore) Complete(context.Context, store.Lease, json.RawMessage) error {
	f.calls = append(f.calls, call{op: "complete"})
	return nil
}
func (f *fakeStore) Fail(_ context.Context, _ store.Lease, msg string, d *time.Duration) error {
	f.calls = append(f.calls, call{op: "fail", msg: msg, delay: d})
	return nil
}
func (f *fakeStore) Release(context.Context, []store.Lease) error { return nil }

func TestClassificationOrder(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name                      string
		lost, cancelReq, shutdown bool
		err                       error
		timedOut                  bool
		attempts                  int
		wantOp, wantMsg, wantOut  string
		wantRetry                 bool
	}{
		{name: "lease lost wins over success", lost: true, wantOut: "lease_lost"},
		{name: "success wins over cancel", cancelReq: true, wantOp: "complete", wantOut: "succeeded"},
		{name: "cancel requested", cancelReq: true, err: boom, wantOp: "fail", wantMsg: "boom", wantOut: "cancelled"},
		{name: "shutdown leaves for release", shutdown: true, err: boom, wantOut: "released"},
		{name: "timeout retries", err: context.DeadlineExceeded, timedOut: true, wantOp: "fail", wantMsg: "timeout", wantRetry: true, wantOut: "failed"},
		{name: "timeout beats permanent", err: handlers.Permanent(boom), timedOut: true, wantOp: "fail", wantMsg: "timeout", wantRetry: true, wantOut: "failed"},
		{name: "permanent is dead", err: handlers.Permanent(boom), wantOp: "fail", wantMsg: "boom", wantOut: "dead"},
		{name: "retryable", err: boom, wantOp: "fail", wantMsg: "boom", wantRetry: true, wantOut: "failed"},
		{name: "retryable at max is dead", err: boom, attempts: 3, wantOp: "fail", wantMsg: "boom", wantOut: "dead"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{}
			p := New(Config{Concurrency: 1, BackoffBase: time.Second, BackoffMax: time.Minute}, fs, nil)
			job := model.Job{ID: uuid.New(), Queue: "q", Type: "noop", Attempts: max(tc.attempts, 1), MaxAttempts: 3}
			a := &active{lease: store.Lease{JobID: job.ID, Token: 7}, cancel: func() {}}
			a.lost.Store(tc.lost)
			a.cancelReq.Store(tc.cancelReq)
			p.shutdown.Store(tc.shutdown)
			p.active[job.ID] = a
			out := p.finish(context.Background(), job, a, nil, tc.err, tc.timedOut)
			assert.Equal(t, tc.wantOut, out)
			if tc.wantOp == "" {
				assert.Empty(t, fs.calls)
				return
			}
			if assert.Len(t, fs.calls, 1) {
				c := fs.calls[0]
				assert.Equal(t, tc.wantOp, c.op)
				assert.Equal(t, tc.wantMsg, c.msg)
				assert.Equal(t, tc.wantRetry, c.delay != nil)
			}
		})
	}
}
