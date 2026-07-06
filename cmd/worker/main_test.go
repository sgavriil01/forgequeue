package main

import "testing"

func TestWorkerCountFromEnvDefault(t *testing.T) {
	count, err := workerCountFromEnv(func(string) string {
		return ""
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if count != 5 {
		t.Fatalf("expected default worker count 5, got %d", count)
	}
}

func TestWorkerCountFromEnvUsesEnv(t *testing.T) {
	count, err := workerCountFromEnv(func(key string) string {
		if key == "FORGEQUEUE_WORKER_COUNT" {
			return "10"
		}

		return ""
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if count != 10 {
		t.Fatalf("expected worker count 10, got %d", count)
	}
}

func TestWorkerCountFromEnvRejectsInvalidValue(t *testing.T) {
	_, err := workerCountFromEnv(func(key string) string {
		if key == "FORGEQUEUE_WORKER_COUNT" {
			return "abc"
		}

		return ""
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWorkerCountFromEnvRejectsZero(t *testing.T) {
	_, err := workerCountFromEnv(func(key string) string {
		if key == "FORGEQUEUE_WORKER_COUNT" {
			return "0"
		}

		return ""
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
