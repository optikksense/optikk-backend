package telemetry

import "database/sql"

// Handler serves OTLP/HTTP ingestion endpoints.
type Handler struct {
	Ingester Ingester
	MySQL    *sql.DB
}

// NewHandler creates a new telemetry handler.
func NewHandler(ingester Ingester, mysql *sql.DB) *Handler {
	return &Handler{Ingester: ingester, MySQL: mysql}
}
