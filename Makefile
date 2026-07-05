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
	
run-api-local:
	FORGEQUEUE_DATABASE_URL="postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable" go run ./cmd/api

LOAD_RATE ?= 20
LOAD_DURATION ?= 30s
LOAD_VUS ?= 20
LOAD_MAX_VUS ?= 100
API_URL ?= http://localhost:8080

.PHONY: load-test
load-test:
	docker run --rm -i --network host \
		-e API_URL="$(API_URL)" \
		-e RATE="$(LOAD_RATE)" \
		-e DURATION="$(LOAD_DURATION)" \
		-e VUS="$(LOAD_VUS)" \
		-e MAX_VUS="$(LOAD_MAX_VUS)" \
		grafana/k6 run - < loadtest/create_jobs.js

LOAD_JOBS ?= 1000
LOAD_VUS ?= 20
LOAD_MAX_DURATION ?= 2m
API_URL ?= http://localhost:8080

.PHONY: load-submit
load-submit:
	docker run --rm -i --network host \
		-e API_URL="$(API_URL)" \
		-e JOBS="$(LOAD_JOBS)" \
		-e VUS="$(LOAD_VUS)" \
		-e MAX_DURATION="$(LOAD_MAX_DURATION)" \
		grafana/k6 run - < load-tests/submit_jobs.js