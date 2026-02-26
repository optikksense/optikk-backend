module github.com/observability/observability-backend-go/modules/ingestion

go 1.24.0

require (
	github.com/IBM/sarama v1.46.3
	github.com/gin-gonic/gin v1.10.0
	github.com/observability/observability-backend-go v0.0.0
	go.opentelemetry.io/proto/otlp v1.9.0
	google.golang.org/protobuf v1.36.10
)

replace github.com/observability/observability-backend-go => ../..
