.PHONY: build test lint vet fmt check migrate-up migrate-down migrate-status migrate-create

build:
	go build -v ./cmd/server

test:
	go test ./... -race -count=1

lint:
	golangci-lint run

vet:
	go vet ./...

fmt:
	gofmt -w .

# Run all checks (lint + test + vet)
check: lint vet test

# Database migrations (requires MIGRATE_DSN env var or -dsn flag)
# Usage: make migrate-up DB=mysql DSN="user:pass@tcp(host:3306)/dbname"
migrate-up:
	go run ./cmd/migrate -db $(DB) -dsn "$(DSN)" up

migrate-down:
	go run ./cmd/migrate -db $(DB) -dsn "$(DSN)" down

migrate-status:
	go run ./cmd/migrate -db $(DB) -dsn "$(DSN)" status

migrate-create:
	go run ./cmd/migrate -db $(DB) -dsn "$(DSN)" create $(NAME) sql
