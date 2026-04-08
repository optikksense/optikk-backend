.PHONY: build run fmt vet

build:
	go build -v ./cmd/server

run:
	go run ./cmd/server

fmt:
	gofmt -w .

vet:
	go vet ./...