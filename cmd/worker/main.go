package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sgavriil01/forgequeue/internal/config"
	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/worker"
)

func main() {
	ctx := context.Background()

	if err := run(ctx, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	workerCount, err := workerCountFromEnv(getenv)
	if err != nil {
		return err
	}

	metricsAddr := getenv("FORGEQUEUE_WORKER_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}

	logger.Info(
		"worker config loaded",
		"worker_count", workerCount,
		"metrics_addr", metricsAddr,
	)

	metricsServer := &http.Server{
		Addr:    metricsAddr,
		Handler: promhttp.Handler(),
	}

	go func() {
		logger.Info("worker metrics server started", "addr", metricsServer.Addr)

		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker metrics server failed", "error", err)
		}
	}()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("worker metrics server shutdown failed", "error", err)
		}
	}()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	queries := db.New(pool)

	registry, err := worker.NewRegistry(
		worker.NewHandlerFunc("test_job", func(ctx context.Context, job db.Job) error {
			logger.Info(
				"handling test job",
				"job_id", job.ID,
				"payload", string(job.Payload),
			)

			// Simulate a tiny bit of work so you can observe the worker.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
				return nil
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("create worker registry: %w", err)
	}

	workerPool := worker.NewExecutorPool(
		queries,
		registry,
		worker.PoolConfig{
			NumWorkers:   workerCount,
			PollInterval: 1 * time.Second,
			IdleJitter:   250 * time.Millisecond,
			LeaseSeconds: 30,
		},
		logger,
	)

	workerPool.Start(ctx)

	logger.Info("worker process started")

	<-ctx.Done()

	logger.Info("shutdown signal received")

	workerPool.Stop()

	logger.Info("worker process stopped")

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func workerCountFromEnv(getenv func(string) string) (int, error) {
	value := getenv("FORGEQUEUE_WORKER_COUNT")
	if value == "" {
		return 5, nil
	}

	count, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("FORGEQUEUE_WORKER_COUNT must be an integer: %w", err)
	}

	if count <= 0 {
		return 0, fmt.Errorf("FORGEQUEUE_WORKER_COUNT must be greater than zero")
	}

	return count, nil
}
