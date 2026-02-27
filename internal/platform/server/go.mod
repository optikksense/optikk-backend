module github.com/observability/observability-backend-go/internal/platform/server

go 1.24.0

require (
	github.com/gin-gonic/gin v1.10.0
	github.com/observability/observability-backend-go v0.0.0
	github.com/swaggo/files v1.0.1
	github.com/swaggo/gin-swagger v1.6.0
	github.com/observability/observability-backend-go/modules/user v0.0.0
	github.com/observability/observability-backend-go/modules/log v0.0.0
	github.com/observability/observability-backend-go/modules/metrics v0.0.0
	github.com/observability/observability-backend-go/modules/spans v0.0.0
	github.com/observability/observability-backend-go/modules/ingestion v0.0.0
	golang.org/x/net v0.50.0
)

replace (
	github.com/observability/observability-backend-go => ../../..
	github.com/observability/observability-backend-go/modules/user => ../../../modules/user
	github.com/observability/observability-backend-go/modules/log => ../../../modules/log
	github.com/observability/observability-backend-go/modules/metrics => ../../../modules/metrics
	github.com/observability/observability-backend-go/modules/spans => ../../../modules/spans
	github.com/observability/observability-backend-go/modules/ingestion => ../../../modules/ingestion
)
