.PHONY: run-api run-worker test lint vet ci compose-up compose-down migrate migrate-down migrate-force sqlc

DB_URL ?= postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

ci: vet lint test

compose-up:
	docker compose up -d

compose-down:
	docker compose down

migrate:
	migrate -path migrations -database "$(DB_URL)" up

migrate-down:
	migrate -path migrations -database "$(DB_URL)" down 1

migrate-force:
	migrate -path migrations -database "$(DB_URL)" force $(VERSION)

sqlc:
	sqlc generate