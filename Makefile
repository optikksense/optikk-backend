.PHONY: build run fmt setup

build:
	go build -v ./cmd/server

run:
	go run ./cmd/server

fmt:
	gofmt -w .

# Set up git hooks (run once after cloning)
setup:
	git config core.hooksPath .githooks
