package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sgavriil01/forgequeue/internal/config"
	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/httpapi"
	"github.com/sgavriil01/forgequeue/internal/jobs"
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	queries := db.New(pool)
	jobService := jobs.NewService(queries)

	apiServer := httpapi.NewServer(jobService, logger)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: apiServer.Routes(),
	}

	errCh := make(chan error, 1)

	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown server: %w", err)
		}

		logger.Info("api stopped")
		return nil

	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}

		return nil
	}
}