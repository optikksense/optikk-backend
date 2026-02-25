package telemetry

import (
	"database/sql"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	telemetryapi "github.com/observability/observability-backend-go/internal/telemetry/api"
	telemetryingest "github.com/observability/observability-backend-go/internal/telemetry/ingest"
	telemetrymodel "github.com/observability/observability-backend-go/internal/telemetry/model"
	telemetrystore "github.com/observability/observability-backend-go/internal/telemetry/store"
)

// Stable public telemetry API re-exported from modular subpackages.
type (
	Handler             = telemetryapi.Handler
	Ingester            = telemetryingest.Ingester
	Repository          = telemetrystore.Repository
	DirectIngester      = telemetryingest.DirectIngester
	KafkaIngester       = telemetryingest.KafkaIngester
	KafkaConsumer       = telemetryingest.KafkaConsumer
	KafkaConsumerConfig = telemetryingest.KafkaConsumerConfig
	SpanRecord          = telemetrymodel.SpanRecord
	MetricRecord        = telemetrymodel.MetricRecord
	LogRecord           = telemetrymodel.LogRecord
)

func NewHandler(ingester Ingester, mysql *sql.DB) *Handler {
	return telemetryapi.NewHandler(ingester, mysql)
}

func NewRepository(db dbutil.Querier) *Repository {
	return telemetrystore.NewRepository(db)
}

func NewDirectIngester(repo *Repository) *DirectIngester {
	return telemetryingest.NewDirectIngester(repo)
}

func NewKafkaIngester(brokers []string) (*KafkaIngester, error) {
	return telemetryingest.NewKafkaIngester(brokers)
}

func NewKafkaConsumer(repo *Repository, cfg KafkaConsumerConfig) (*KafkaConsumer, error) {
	return telemetryingest.NewKafkaConsumer(repo, cfg)
}
