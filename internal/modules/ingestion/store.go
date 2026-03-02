package telemetry

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

// BatchInsertResult reports how many records were accepted vs rejected during
// a batch insert. When RejectedCount > 0, ErrorMessage contains a summary of
// the first failure encountered.
type BatchInsertResult struct {
	TotalCount    int
	AcceptedCount int
	RejectedCount int
	ErrorMessage  string
}

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

func (r *Repository) InsertSpans(ctx context.Context, spans []SpanRecord) (*BatchInsertResult, error) {
	result := &BatchInsertResult{TotalCount: len(spans)}
	if len(spans) == 0 {
		return result, nil
	}
	log.Printf("otlp: inserting %d spans", len(spans))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		result.RejectedCount = len(spans)
		result.ErrorMessage = fmt.Sprintf("begin tx: %v", err)
		return result, err
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
		result.RejectedCount = len(spans)
		result.ErrorMessage = fmt.Sprintf("prepare: %v", err)
		return result, err
	}
	defer stmt.Close()

	var firstErr string
	for _, s := range spans {
		if _, err := stmt.ExecContext(ctx,
			s.TeamUUID, s.TraceID, s.SpanID, s.ParentSpanID, s.ParentServiceName, s.IsRoot,
			s.OperationName, s.ServiceName, s.SpanKind,
			s.StartTime, s.EndTime, s.DurationMs,
			s.Status, s.StatusMessage,
			s.HTTPMethod, s.HTTPURL, s.HTTPStatusCode,
			s.Host, s.Pod, s.Container, s.Attributes,
		); err != nil {
			result.RejectedCount++
			if firstErr == "" {
				firstErr = fmt.Sprintf("span %s/%s: %v", s.TraceID, s.SpanID, err)
			}
			log.Printf("otlp: span append %s/%s: %v", s.TraceID, s.SpanID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		result.RejectedCount = len(spans)
		result.AcceptedCount = 0
		result.ErrorMessage = fmt.Sprintf("commit: %v", err)
		return result, err
	}

	result.AcceptedCount = len(spans) - result.RejectedCount
	result.ErrorMessage = firstErr
	return result, nil
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func (r *Repository) InsertMetrics(ctx context.Context, metrics []MetricRecord) (*BatchInsertResult, error) {
	result := &BatchInsertResult{TotalCount: len(metrics)}
	if len(metrics) == 0 {
		return result, nil
	}
	log.Printf("otlp: inserting %d metrics", len(metrics))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		result.RejectedCount = len(metrics)
		result.ErrorMessage = fmt.Sprintf("begin tx: %v", err)
		return result, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO metrics (
		id, team_id, metric_id, metric_name, metric_type, metric_category,
		service_name, operation_name, timestamp,
		value, count, sum, min, max, avg,
		p50, p95, p99,
		http_method, http_status_code, status,
		host, pod, container, attributes
	)`)
	if err != nil {
		result.RejectedCount = len(metrics)
		result.ErrorMessage = fmt.Sprintf("prepare: %v", err)
		return result, err
	}
	defer stmt.Close()

	var firstErr string
	for _, m := range metrics {
		if _, err := stmt.ExecContext(ctx,
			nextMetricID(m.Timestamp), m.TeamUUID, metricSeriesID(m), m.MetricName, m.MetricType, m.MetricCategory,
			m.ServiceName, m.OperationName, m.Timestamp,
			m.Value, m.Count, m.Sum, m.Min, m.Max, m.Avg,
			m.P50, m.P95, m.P99,
			m.HTTPMethod, m.HTTPStatusCode, m.Status,
			m.Host, m.Pod, m.Container, m.Attributes,
		); err != nil {
			result.RejectedCount++
			if firstErr == "" {
				firstErr = fmt.Sprintf("metric %s: %v", m.MetricName, err)
			}
			log.Printf("otlp: metric append %s: %v", m.MetricName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		result.RejectedCount = len(metrics)
		result.AcceptedCount = 0
		result.ErrorMessage = fmt.Sprintf("commit: %v", err)
		return result, err
	}

	result.AcceptedCount = len(metrics) - result.RejectedCount
	result.ErrorMessage = firstErr
	return result, nil
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

func (r *Repository) InsertLogs(ctx context.Context, logs []LogRecord) (*BatchInsertResult, error) {
	result := &BatchInsertResult{TotalCount: len(logs)}
	if len(logs) == 0 {
		return result, nil
	}
	log.Printf("otlp: inserting %d logs", len(logs))

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		result.RejectedCount = len(logs)
		result.ErrorMessage = fmt.Sprintf("begin tx: %v", err)
		return result, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO logs (
		id, team_id, timestamp, level, service_name, logger, message,
		trace_id, span_id, host, pod, container, thread, exception, attributes
	)`)
	if err != nil {
		result.RejectedCount = len(logs)
		result.ErrorMessage = fmt.Sprintf("prepare: %v", err)
		return result, err
	}
	defer stmt.Close()

	var firstErr string
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
			result.RejectedCount++
			if firstErr == "" {
				firstErr = fmt.Sprintf("log record: %v", err)
			}
			log.Printf("otlp: log append: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		result.RejectedCount = len(logs)
		result.AcceptedCount = 0
		result.ErrorMessage = fmt.Sprintf("commit: %v", err)
		return result, err
	}

	result.AcceptedCount = len(logs) - result.RejectedCount
	result.ErrorMessage = firstErr
	return result, nil
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
var metricIDSequence uint64

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

// nextMetricID generates a sortable UInt64 id for metric rows.
func nextMetricID(ts time.Time) uint64 {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	micros := ts.UTC().UnixMicro()
	if micros < 0 {
		micros = time.Now().UTC().UnixMicro()
	}
	seq := atomic.AddUint64(&metricIDSequence, 1) & logIDSequenceMask
	id := (uint64(micros) << logIDSequenceBits) | seq
	if id == 0 {
		return 1
	}
	return id
}

// metricSeriesID returns a stable id used for grouping similar metric rows.
func metricSeriesID(m MetricRecord) string {
	h := fnv.New64a()
	writeMetricIDPart := func(part string) {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}

	writeMetricIDPart(m.TeamUUID)
	writeMetricIDPart(m.MetricName)
	writeMetricIDPart(m.MetricType)
	writeMetricIDPart(m.MetricCategory)
	writeMetricIDPart(m.ServiceName)
	writeMetricIDPart(m.OperationName)
	writeMetricIDPart(m.HTTPMethod)
	writeMetricIDPart(m.Status)
	writeMetricIDPart(m.Host)
	writeMetricIDPart(m.Pod)
	writeMetricIDPart(m.Container)
	writeMetricIDPart(m.Attributes)

	return fmt.Sprintf("%016x", h.Sum64())
}

func shouldPersistMetric(_ string) bool { return true }
