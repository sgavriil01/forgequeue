package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sgavriil01/forgequeue/internal/config"
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

	logger.Info(
		"worker starting",
		"env", cfg.Env,
	)

	<-ctx.Done()

	logger.Info("worker stopped")

	return nil
}