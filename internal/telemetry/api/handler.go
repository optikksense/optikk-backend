package api

import (
	"database/sql"

	"github.com/observability/observability-backend-go/internal/telemetry/ingest"
)

// Handler serves OTLP/HTTP ingestion endpoints.
type Handler struct {
	Ingester ingest.Ingester
	MySQL    *sql.DB
}

// NewHandler creates a new telemetry handler.
func NewHandler(ingester ingest.Ingester, mysql *sql.DB) *Handler {
	return &Handler{Ingester: ingester, MySQL: mysql}
}
