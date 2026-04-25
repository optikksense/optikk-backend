.PHONY: build run fmt vet proto loadtest-smoke loadtest-all loadtest-traces loadtest-logs loadtest-metrics loadtest-overview loadtest-infrastructure loadtest-saturation loadtest-services

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

# Query-side load test — see loadtest/docs/README.md for details.
# Override credentials with EMAIL=... PASSWORD=... ; tune with RPS=... DURATION=... VUS=...
LOADTEST_EMAIL    ?= loadtest@optikk.local
LOADTEST_PASSWORD ?= optikk-loadtest
LOADTEST_RPS      ?= 10
LOADTEST_DURATION ?= 1m
LOADTEST_VUS      ?= 50
LOADTEST_ENV      = -e EMAIL=$(LOADTEST_EMAIL) -e PASSWORD=$(LOADTEST_PASSWORD) \
                    -e RPS=$(LOADTEST_RPS) -e DURATION=$(LOADTEST_DURATION) -e VUS=$(LOADTEST_VUS)

loadtest-smoke:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/smoke.js

loadtest-all:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/all.js

loadtest-traces:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/traces.js

loadtest-logs:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/logs.js

loadtest-metrics:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/metrics.js

loadtest-overview:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/overview.js

loadtest-infrastructure:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/infrastructure.js

loadtest-saturation:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/saturation.js

loadtest-services:
	k6 run $(LOADTEST_ENV) loadtest/entrypoints/services.js