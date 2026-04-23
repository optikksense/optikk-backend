.PHONY: build run fmt vet proto

proto:
	PATH="$(shell go env GOPATH)/bin:$(PATH)" go generate ./...

build:
	go build -v ./cmd/server

run:
	go run ./cmd/server

fmt:
	gofmt -w .

vet:
	go vet ./...