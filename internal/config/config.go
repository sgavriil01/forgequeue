package config

import (
	"fmt"
	"time"
)

type Config struct {
	Env             string
	HTTPAddr        string
	DatabaseURL     string
	ShutdownTimeout time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Env:             getDefault(getenv, "FORGEQUEUE_ENV", "development"),
		HTTPAddr:        getDefault(getenv, "FORGEQUEUE_HTTP_ADDR", ":8080"),
		DatabaseURL:     getenv("FORGEQUEUE_DATABASE_URL"),
		ShutdownTimeout: 10 * time.Second,
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("FORGEQUEUE_DATABASE_URL is required")
	}

	if raw := getenv("FORGEQUEUE_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEQUEUE_SHUTDOWN_TIMEOUT: %w", err)
		}

		cfg.ShutdownTimeout = d
	}

	return cfg, nil
}

func getDefault(getenv func(string) string, key string, fallback string) string {
	if value := getenv(key); value != "" {
		return value
	}

	return fallback
}