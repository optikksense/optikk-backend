// Package selftelemetry implements Fix 19: Self-Instrumentation.
//
// Optic reports its own spans, logs, and metrics to the same ClickHouse
// instance it uses for customer data. All self-telemetry rows are written
// with team_id = 0 (the "optic-platform" virtual tenant) so they never
// pollute customer queries.
//
// The ClickHouse user / password / host must come from the app's existing
// config.Config — no hardcoded credentials.  Configure via:
//
//	CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE,
//	CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD
//
// Usage (in app.go server startup):
//
//	st := selftelemetry.New(cfg)
//	defer st.Shutdown(ctx)
//	st.RecordHTTPRequest(r.Method, r.URL.Path, status, latency)
//	st.RecordDBQuery("clickhouse", "SELECT ...", latency, err)
package selftelemetry

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/observability/observability-backend-go/internal/config"
)

const (
	selfTeamID    int64 = 0             // virtual tenant for self-data
	selfTable           = "optic_spans" // shared spans table
	logTable            = "optic_logs"  // shared logs table (if present)
	batchSize           = 100
	flushInterval       = 5 * time.Second
)

// SpanRow is a minimal OTLP-like span for self-telemetry.
type SpanRow struct {
	Timestamp      time.Time
	TraceID        string
	SpanID         string
	ParentSpanID   string
	ServiceName    string
	OperationName  string
	DurationMs     float64
	StatusCode     int
	AttributesJSON string
}

// Telemetry collects and bulk-inserts self-instrumentation data.
type Telemetry struct {
	db       *sql.DB
	hostname string
	version  string
	spans    chan SpanRow
	done     chan struct{}
}

// New creates a Telemetry instance connected to the existing ClickHouse DB.
// Credentials come entirely from cfg — no literals.
func New(cfg config.Config) *Telemetry {
	dsn := fmt.Sprintf(
		"clickhouse://%s:%s@%s:%s/%s?secure=false",
		cfg.ClickHouseUser, cfg.ClickHousePassword,
		cfg.ClickHouseHost, cfg.ClickHousePort,
		cfg.ClickHouseDatabase,
	)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Printf("selftelemetry: failed to open ClickHouse: %v — self-instrumentation disabled", err)
		return &Telemetry{done: make(chan struct{})}
	}

	hostname, _ := os.Hostname()
	t := &Telemetry{
		db:       db,
		hostname: hostname,
		version:  getEnv("APP_VERSION", "dev"),
		spans:    make(chan SpanRow, 1024),
		done:     make(chan struct{}),
	}
	go t.flushLoop()
	return t
}

// RecordHTTPRequest records an HTTP request span into self-telemetry.
// Call this from a Gin middleware after the handler returns.
func (t *Telemetry) RecordHTTPRequest(method, path string, statusCode int, latencyMs float64) {
	if t.db == nil {
		return
	}
	t.enqueue(SpanRow{
		Timestamp:     time.Now().UTC(),
		TraceID:       uuid.NewString(),
		SpanID:        uuid.NewString(),
		ServiceName:   "optic-backend",
		OperationName: fmt.Sprintf("HTTP %s %s", method, path),
		DurationMs:    latencyMs,
		StatusCode:    statusCode,
		AttributesJSON: fmt.Sprintf(
			`{"host":%q,"version":%q,"method":%q,"path":%q}`,
			t.hostname, t.version, method, path,
		),
	})
}

// RecordDBQuery records a database query span.
func (t *Telemetry) RecordDBQuery(dbType, query string, latencyMs float64, err error) {
	if t.db == nil {
		return
	}
	status := 0
	attrs := fmt.Sprintf(`{"db_type":%q,"query":%q,"host":%q}`, dbType, truncate(query, 200), t.hostname)
	if err != nil {
		status = 2
		attrs = fmt.Sprintf(`{"db_type":%q,"error":%q,"host":%q}`, dbType, err.Error(), t.hostname)
	}
	t.enqueue(SpanRow{
		Timestamp:      time.Now().UTC(),
		TraceID:        uuid.NewString(),
		SpanID:         uuid.NewString(),
		ServiceName:    "optic-backend",
		OperationName:  fmt.Sprintf("db.%s.query", dbType),
		DurationMs:     latencyMs,
		StatusCode:     status,
		AttributesJSON: attrs,
	})
}

func (t *Telemetry) enqueue(s SpanRow) {
	select {
	case t.spans <- s:
	default:
		// Drop oldest — never block the caller.
	}
}

func (t *Telemetry) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]SpanRow, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := t.insertBatch(batch); err != nil {
			log.Printf("selftelemetry: insert error: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case s := <-t.spans:
			batch = append(batch, s)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-t.done:
			flush()
			return
		}
	}
}

func (t *Telemetry) insertBatch(rows []SpanRow) error {
	if t.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use INSERT INTO ... VALUES (...) — ClickHouse native batch.
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			team_id, timestamp, trace_id, span_id, parent_span_id,
			service_name, operation_name, duration_ms, status_code, attributes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, selfTable))
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		_, err := stmt.ExecContext(ctx,
			selfTeamID,
			r.Timestamp,
			r.TraceID, r.SpanID, r.ParentSpanID,
			r.ServiceName, r.OperationName,
			r.DurationMs, r.StatusCode,
			r.AttributesJSON,
		)
		if err != nil {
			log.Printf("selftelemetry: row insert error: %v", err)
		}
	}
	return tx.Commit()
}

// Shutdown flushes remaining spans and closes the DB connection.
func (t *Telemetry) Shutdown(ctx context.Context) {
	if t.done != nil {
		close(t.done)
	}
	if t.db != nil {
		t.db.Close()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
