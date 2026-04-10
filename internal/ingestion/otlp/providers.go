package otlp

import (
	"database/sql"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
)

func NewByteTracker(db *sql.DB, flushInterval time.Duration) platformingestion.SizeTracker {
	return ingest.NewByteTracker(db, flushInterval)
}
