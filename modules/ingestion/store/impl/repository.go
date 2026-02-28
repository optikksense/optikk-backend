package impl

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// ClickHouseRepository persists telemetry records into ClickHouse.
type ClickHouseRepository struct {
	DB dbutil.Querier
}

const logIDSequenceBits = 12
const logIDSequenceMask = (1 << logIDSequenceBits) - 1

var logIDSequence uint64

// NewRepository creates a telemetry repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{DB: db}
}

func (r *ClickHouseRepository) InsertSpans(ctx context.Context, spans []model.SpanRecord) error {
	if len(spans) == 0 {
		return nil
	}
	log.Printf("otlp: attempting to insert %d spans", len(spans))

	const chunkSize = 100
	for start := 0; start < len(spans); start += chunkSize {
		end := start + chunkSize
		if end > len(spans) {
			end = len(spans)
		}
		if err := r.insertSpansChunk(ctx, spans[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClickHouseRepository) insertSpansChunk(ctx context.Context, spans []model.SpanRecord) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, span := range spans {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO spans (
				team_id, trace_id, span_id, parent_span_id, is_root,
				operation_name, service_name, span_kind,
				start_time, end_time, duration_ms,
				status, status_message,
				http_method, http_url, http_status_code,
				host, pod, container, attributes
			) VALUES (
				?, ?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?,
				?, ?,
				?, ?, ?,
				?, ?, ?, ?
			)`,
			span.TeamUUID, span.TraceID, span.SpanID, nullStr(span.ParentSpanID), span.IsRoot,
			span.OperationName, span.ServiceName, span.SpanKind,
			span.StartTime, span.EndTime, span.DurationMs,
			span.Status, nullStr(span.StatusMessage),
			nullStr(span.HTTPMethod), nullStr(span.HTTPURL), nullInt(span.HTTPStatusCode),
			nullStr(span.Host), nullStr(span.Pod), nullStr(span.Container),
			span.Attributes,
		)
		if err != nil {
			log.Printf("otlp: add span to batch %s/%s failed: %v", span.TraceID, span.SpanID, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch insertion: %w", err)
	}
	return nil
}

func (r *ClickHouseRepository) InsertMetrics(ctx context.Context, metrics []model.MetricRecord) error {
	filtered := make([]model.MetricRecord, 0, len(metrics))
	for _, rec := range metrics {
		if shouldPersistMetric(rec.MetricName) {
			filtered = append(filtered, rec)
		}
	}
	metrics = filtered

	if len(metrics) == 0 {
		return nil
	}

	const metricInsertChunkSize = 100
	for start := 0; start < len(metrics); start += metricInsertChunkSize {
		end := start + metricInsertChunkSize
		if end > len(metrics) {
			end = len(metrics)
		}
		if err := r.insertMetricsChunk(ctx, metrics[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClickHouseRepository) insertMetricsChunk(ctx context.Context, metrics []model.MetricRecord) error {
	if len(metrics) == 0 {
		return nil
	}

	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	const valuesPerRow = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(`
			INSERT INTO metrics (
				team_id, metric_name, metric_type, metric_category,
				service_name, operation_name, timestamp,
				value, count, sum, min, max, avg,
				p50, p95, p99,
				http_method, http_status_code, status,
				host, pod, container, attributes
			) VALUES `)

	args := make([]any, 0, len(metrics)*23)
	for _, rec := range metrics {
		if len(args) > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(valuesPerRow)
		args = append(args,
			rec.TeamUUID, rec.MetricName, rec.MetricType, rec.MetricCategory,
			rec.ServiceName, nullStr(rec.OperationName), rec.Timestamp,
			rec.Value, rec.Count, rec.Sum, rec.Min, rec.Max, rec.Avg,
			rec.P50, rec.P95, rec.P99,
			nullStr(rec.HTTPMethod), nullInt(rec.HTTPStatusCode), rec.Status,
			nullStr(rec.Host), nullStr(rec.Pod), nullStr(rec.Container),
			rec.Attributes,
		)
	}

	if _, err := tx.ExecContext(ctx, queryBuilder.String(), args...); err != nil {
		return fmt.Errorf("insert metrics chunk (%d rows): %w", len(metrics), err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch insertion: %w", err)
	}
	return nil
}

func (r *ClickHouseRepository) InsertLogs(ctx context.Context, logs []model.LogRecord) error {
	if len(logs) == 0 {
		return nil
	}

	const chunkSize = 100
	for start := 0; start < len(logs); start += chunkSize {
		end := start + chunkSize
		if end > len(logs) {
			end = len(logs)
		}
		if err := r.insertLogsChunk(ctx, logs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClickHouseRepository) insertLogsChunk(ctx context.Context, logs []model.LogRecord) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

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

		_, err := tx.ExecContext(ctx, `
			INSERT INTO logs (
				id, team_id, timestamp, level, service_name, logger, message, trace_id, span_id,
				host, pod, container, thread, exception, attributes
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?
			)`,
			nextLogID(rec.Timestamp), rec.TeamUUID, rec.Timestamp, level, service,
			nullStr(rec.Logger), message, nullStr(rec.TraceID), nullStr(rec.SpanID),
			nullStr(rec.Host), nullStr(rec.Pod), nullStr(rec.Container),
			nullStr(rec.Thread), nullStr(rec.Exception), rec.Attributes,
		)
		if err != nil {
			log.Printf("otlp: add log to batch failed: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch insertion: %w", err)
	}
	return nil
}

func nullStr(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullInt(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

func normalizeLevel(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
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

// nextLogID generates a sortable UInt64 id for log rows.
// Layout: unix microseconds in high bits + per-process sequence in low bits.
func nextLogID(ts time.Time) uint64 {
	if ts.IsZero() {
		ts = time.Now().UTC()
	} else {
		ts = ts.UTC()
	}

	micros := ts.UnixMicro()
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

func shouldPersistMetric(metricName string) bool {
	_ = metricName
	return true
}
