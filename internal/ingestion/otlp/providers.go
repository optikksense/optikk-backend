package otlp

import (
	"database/sql"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
)

func NewByteTracker(db *sql.DB, flushInterval time.Duration) ingestion.SizeTracker {
	return ingest.NewByteTracker(db, flushInterval)
}
