.PHONY: build run fmt

build:
	go build -v ./cmd/server

run:
	go run ./cmd/server

fmt:
	gofmt -w .
