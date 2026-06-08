package config

import (
	"testing"
	"time"
)

func TestLoadUsesDefaults(t *testing.T) {
	env := map[string]string{
		"FORGEQUEUE_DATABASE_URL": "postgres://example",
	}

	cfg, err := Load(func(key string) string {
		return env[key]
	})
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Env != "development" {
		t.Fatalf("Env = %q, want %q", cfg.Env, "development")
	}

	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}

	if cfg.DatabaseURL != "postgres://example" {
		t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, "postgres://example")
	}

	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 10*time.Second)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	_, err := Load(func(string) string {
		return ""
	})
	if err == nil {
		t.Fatal("Load returned nil error, want error")
	}
}

func TestLoadParsesShutdownTimeout(t *testing.T) {
	env := map[string]string{
		"FORGEQUEUE_DATABASE_URL":       "postgres://example",
		"FORGEQUEUE_SHUTDOWN_TIMEOUT": "5s",
	}

	cfg, err := Load(func(key string) string {
		return env[key]
	})
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.ShutdownTimeout != 5*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 5*time.Second)
	}
}