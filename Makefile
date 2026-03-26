.PHONY: build lint vet fmt check

build:
	go build -v ./cmd/server

run:
	go run ./cmd/server

lint:
	golangci-lint run

vet:
	go vet ./...

fmt:
	gofmt -w .

# Run all checks (lint + vet + build)
check: lint vet build
