module github.com/observability/observability-backend-go/cmd/server

go 1.24.0

require (
	github.com/observability/observability-backend-go v0.0.0
	github.com/observability/observability-backend-go/internal/platform/server v0.0.0
)

replace (
	github.com/observability/observability-backend-go => ../..
	github.com/observability/observability-backend-go/internal/platform/server => ../../internal/platform/server
)
