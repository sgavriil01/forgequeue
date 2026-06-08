.PHONY: run-api run-worker test lint compose-up compose-down

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

test:
	go test ./...

lint:
	golangci-lint run

compose-up:
	docker compose up -d

compose-down:
	docker compose down