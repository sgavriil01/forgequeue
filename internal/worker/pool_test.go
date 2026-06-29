package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeOneShotExecutor struct {
	fn func(ctx context.Context) (bool, error)
}

func (f fakeOneShotExecutor) ExecuteOnce(ctx context.Context) (bool, error) {
	return f.fn(ctx)
}

func TestPoolStartsConfiguredNumberOfWorkers(t *testing.T) {
	var factoryCalls int32

	pool := NewPool(PoolConfig{
		NumWorkers:   3,
		PollInterval: 5 * time.Millisecond,
		IdleJitter:   1 * time.Millisecond,
	}, func(workerID string) OneShotExecutor {
		atomic.AddInt32(&factoryCalls, 1)

		return fakeOneShotExecutor{
			fn: func(ctx context.Context) (bool, error) {
				return false, nil
			},
		}
	}, nil)

	pool.Start(context.Background())

	waitUntil(t, func() bool {
		return atomic.LoadInt32(&factoryCalls) == 3
	})

	pool.Stop()

	if got := atomic.LoadInt32(&factoryCalls); got != 3 {
		t.Fatalf("expected 3 workers, got %d", got)
	}
}

func TestPoolCallsExecutorRepeatedly(t *testing.T) {
	var calls int32

	pool := NewPool(PoolConfig{
		NumWorkers:   1,
		PollInterval: 5 * time.Millisecond,
		IdleJitter:   1 * time.Millisecond,
	}, func(workerID string) OneShotExecutor {
		return fakeOneShotExecutor{
			fn: func(ctx context.Context) (bool, error) {
				atomic.AddInt32(&calls, 1)
				return false, nil
			},
		}
	}, nil)

	pool.Start(context.Background())

	waitUntil(t, func() bool {
		return atomic.LoadInt32(&calls) >= 2
	})

	pool.Stop()

	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("expected executor to be called repeatedly, got %d calls", got)
	}
}

func TestPoolContinuesAfterExecutorError(t *testing.T) {
	var calls int32

	pool := NewPool(PoolConfig{
		NumWorkers:   1,
		PollInterval: 5 * time.Millisecond,
		IdleJitter:   1 * time.Millisecond,
	}, func(workerID string) OneShotExecutor {
		return fakeOneShotExecutor{
			fn: func(ctx context.Context) (bool, error) {
				atomic.AddInt32(&calls, 1)
				return false, errors.New("temporary executor error")
			},
		}
	}, nil)

	pool.Start(context.Background())

	waitUntil(t, func() bool {
		return atomic.LoadInt32(&calls) >= 2
	})

	pool.Stop()

	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("expected pool to continue after executor error, got %d calls", got)
	}
}

func TestPoolStopsWhenContextIsCancelled(t *testing.T) {
	var calls int32

	ctx, cancel := context.WithCancel(context.Background())

	pool := NewPool(PoolConfig{
		NumWorkers:   1,
		PollInterval: 5 * time.Millisecond,
		IdleJitter:   1 * time.Millisecond,
	}, func(workerID string) OneShotExecutor {
		return fakeOneShotExecutor{
			fn: func(ctx context.Context) (bool, error) {
				atomic.AddInt32(&calls, 1)
				return false, nil
			},
		}
	}, nil)

	pool.Start(ctx)

	waitUntil(t, func() bool {
		return atomic.LoadInt32(&calls) >= 1
	})

	cancel()

	done := make(chan struct{})

	go func() {
		pool.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("pool did not stop after context cancellation")
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}