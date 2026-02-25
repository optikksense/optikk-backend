package telemetry

import "database/sql"

// Handler serves OTLP/HTTP ingestion endpoints.
type Handler struct {
	Repo  *Repository
	MySQL *sql.DB
}

// NewHandler creates a new telemetry handler.
func NewHandler(repo *Repository, mysql *sql.DB) *Handler {
	return &Handler{Repo: repo, MySQL: mysql}
}
