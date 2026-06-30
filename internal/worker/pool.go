package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	defaultNumWorkers   = 5
	defaultPollInterval = 1 * time.Second
	defaultIdleJitter   = 250 * time.Millisecond
	defaultLeaseSeconds = int32(30)
)

type OneShotExecutor interface {
	ExecuteOnce(ctx context.Context) (bool, error)
}

type ExecutorFactory func(workerID string) OneShotExecutor

type PoolConfig struct {
	NumWorkers   int
	PollInterval time.Duration
	IdleJitter   time.Duration
	LeaseSeconds int32
}

type Pool struct {
	config  PoolConfig
	factory ExecutorFactory
	logger  *slog.Logger

	mu     sync.Mutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
	running bool
}

func NewPool(config PoolConfig, factory ExecutorFactory, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}

	if config.NumWorkers <= 0 {
		config.NumWorkers = defaultNumWorkers
	}

	if config.PollInterval <= 0 {
		config.PollInterval = defaultPollInterval
	}

	if config.IdleJitter < 0 {
		config.IdleJitter = 0
	}

	if config.IdleJitter == 0 {
		config.IdleJitter = defaultIdleJitter
	}

	if config.LeaseSeconds <= 0 {
		config.LeaseSeconds = defaultLeaseSeconds
	}

	return &Pool{
		config:  config,
		factory: factory,
		logger:  logger,
	}
}

func NewExecutorPool(store Store, registry *Registry, config PoolConfig, logger *slog.Logger) *Pool {
	return NewPool(config, func(workerID string) OneShotExecutor {
		return NewExecutor(store, registry, workerID, config.LeaseSeconds, logger)
	}, logger)
}

func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.running = true

	p.logger.Info("worker pool starting", "num_workers", p.config.NumWorkers)

	for i := 0; i < p.config.NumWorkers; i++ {
		workerID := newWorkerID(i)

		p.wg.Add(1)

		go p.runWorker(ctx, workerID)
	}
}

func (p *Pool) Stop() {
	p.mu.Lock()

	if !p.running {
		p.mu.Unlock()
		return
	}

	cancel := p.cancel
	p.running = false
	p.cancel = nil

	p.mu.Unlock()

	p.logger.Info("worker pool stopping")

	if cancel != nil {
		cancel()
	}

	p.wg.Wait()

	p.logger.Info("worker pool stopped")
}

func (p *Pool) runWorker(ctx context.Context, workerID string) {
	defer p.wg.Done()

	executor := p.factory(workerID)

	p.logger.Info("worker started", "worker_id", workerID)

	defer p.logger.Info("worker stopped", "worker_id", workerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		processed, err := executor.ExecuteOnce(context.Background())
		if err != nil {
			p.logger.Error("worker execute failed", "worker_id", workerID, "error", err)
		}

		if !processed {
			if !sleepWithContext(ctx, p.idleDelay()) {
				return
			}
		}
	}
}

func (p *Pool) idleDelay() time.Duration {
	if p.config.IdleJitter <= 0 {
		return p.config.PollInterval
	}

	jitter := time.Duration(rand.Int63n(int64(p.config.IdleJitter)))

	return p.config.PollInterval + jitter
}

func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newWorkerID(index int) string {
	return fmt.Sprintf("worker-%d-%s", index, uuid.NewString())
}