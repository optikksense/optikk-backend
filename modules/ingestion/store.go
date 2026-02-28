package telemetry

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

// Repository persists telemetry records into ClickHouse.
// It uses the clickhouse-go/v2 SQL driver's batch mode:
// BeginTx + PrepareContext + ExecContext per row + Commit sends one batch.
type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// ---------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------

func (r *Repository) InsertSpans(ctx context.Context, spans []SpanRecord) error {
	if len(spans) == 0 {
		return nil
	}
	log.Printf("otlp: inserting %d spans", len(spans))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO spans (
		team_id, trace_id, span_id, parent_span_id, parent_service_name, is_root,
		operation_name, service_name, span_kind,
		start_time, end_time, duration_ms,
		status, status_message,
		http_method, http_url, http_status_code,
		host, pod, container, attributes
	)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, s := range spans {
		if _, err := stmt.ExecContext(ctx,
			s.TeamUUID, s.TraceID, s.SpanID, s.ParentSpanID, s.ParentServiceName, s.IsRoot,
			s.OperationName, s.ServiceName, s.SpanKind,
			s.StartTime, s.EndTime, s.DurationMs,
			s.Status, s.StatusMessage,
			s.HTTPMethod, s.HTTPURL, s.HTTPStatusCode,
			s.Host, s.Pod, s.Container, s.Attributes,
		); err != nil {
			log.Printf("otlp: span append %s/%s: %v", s.TraceID, s.SpanID, err)
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func (r *Repository) InsertMetrics(ctx context.Context, metrics []MetricRecord) error {
	if len(metrics) == 0 {
		return nil
	}
	log.Printf("otlp: inserting %d metrics", len(metrics))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO metrics (
		team_id, metric_name, metric_type, metric_category,
		service_name, operation_name, timestamp,
		value, count, sum, min, max, avg,
		p50, p95, p99,
		http_method, http_status_code, status,
		host, pod, container, attributes
	)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range metrics {
		if _, err := stmt.ExecContext(ctx,
			m.TeamUUID, m.MetricName, m.MetricType, m.MetricCategory,
			m.ServiceName, m.OperationName, m.Timestamp,
			m.Value, m.Count, m.Sum, m.Min, m.Max, m.Avg,
			m.P50, m.P95, m.P99,
			m.HTTPMethod, m.HTTPStatusCode, m.Status,
			m.Host, m.Pod, m.Container, m.Attributes,
		); err != nil {
			log.Printf("otlp: metric append %s: %v", m.MetricName, err)
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

func (r *Repository) InsertLogs(ctx context.Context, logs []LogRecord) error {
	if len(logs) == 0 {
		return nil
	}
	log.Printf("otlp: inserting %d logs", len(logs))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO logs (
		id, team_id, timestamp, level, service_name, logger, message,
		trace_id, span_id, host, pod, container, thread, exception, attributes
	)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, rec := range logs {
		level := normalizeLevel(rec.Level)
		service := strings.TrimSpace(rec.Service)
		if service == "" {
			service = "unknown"
		}
		message := strings.TrimSpace(rec.Message)
		if message == "" {
			message = "otlp log record"
		}
		if _, err := stmt.ExecContext(ctx,
			nextLogID(rec.Timestamp), rec.TeamUUID, rec.Timestamp, level, service,
			rec.Logger, message, rec.TraceID, rec.SpanID,
			rec.Host, rec.Pod, rec.Container, rec.Thread, rec.Exception, rec.Attributes,
		); err != nil {
			log.Printf("otlp: log append: %v", err)
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func normalizeLevel(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG", "TRACE":
		return "DEBUG"
	case "WARN", "WARNING":
		return "WARN"
	case "ERROR":
		return "ERROR"
	case "FATAL":
		return "FATAL"
	default:
		return "INFO"
	}
}

const (
	logIDSequenceBits = 12
	logIDSequenceMask = (1 << logIDSequenceBits) - 1
)

var logIDSequence uint64

// nextLogID generates a sortable UInt64 id for log rows.
// Layout: unix microseconds in high bits + per-process sequence in low 12 bits.
func nextLogID(ts time.Time) uint64 {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	micros := ts.UTC().UnixMicro()
	if micros < 0 {
		micros = time.Now().UTC().UnixMicro()
	}
	seq := atomic.AddUint64(&logIDSequence, 1) & logIDSequenceMask
	id := (uint64(micros) << logIDSequenceBits) | seq
	if id == 0 {
		return 1
	}
	return id
}

func shouldPersistMetric(_ string) bool { return true }
