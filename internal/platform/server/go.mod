module github.com/observability/observability-backend-go/internal/platform/server

go 1.24.0

require (
	github.com/gin-gonic/gin v1.10.0
	github.com/observability/observability-backend-go v0.0.0
	github.com/swaggo/files v1.0.1
	github.com/swaggo/gin-swagger v1.6.0
	golang.org/x/net v0.50.0
)

replace (
	github.com/observability/observability-backend-go => ../../..
)
