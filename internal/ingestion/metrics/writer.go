package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
)

const chTable = "observability.metrics"

var chColumns = []string{
	"team_id", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint", "timestamp",
	"ts_bucket",
	"value", "hist_sum", "hist_count", "hist_buckets", "hist_counts",
	"service", "host", "environment", "k8s_namespace", "http_method", "http_status_code",
	"resource", "attributes",
}

type Writer struct {
	ch    clickhouse.Conn
	query string
}

func NewWriter(ch clickhouse.Conn) *Writer {
	return &Writer{
		ch:    ch,
		query: "INSERT INTO " + chTable + " (" + strings.Join(chColumns, ", ") + ")",
	}
}

func (w *Writer) Insert(ctx context.Context, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          uint8(1),
		"wait_for_async_insert": uint8(1),
	}))
	batch, err := w.ch.PrepareBatch(ctx, w.query)
	if err != nil {
		return fmt.Errorf("metrics writer: prepare: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(rowValues(r)...); err != nil {
			return fmt.Errorf("metrics writer: append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("metrics writer: send: %w", err)
	}
	return nil
}

func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetMetricName(),
		r.GetMetricType(),
		r.GetTemporality(),
		r.GetIsMonotonic(),
		r.GetUnit(),
		r.GetDescription(),
		r.GetFingerprint(),
		time.Unix(0, r.GetTimestampNs()),
		uint32(r.GetTsBucketHourSeconds()), //nolint:gosec // ts_bucket is now 5-min-aligned UInt32 Unix-seconds; field name kept for proto wire-compat

		r.GetValue(),
		r.GetHistSum(),
		r.GetHistCount(),
		r.GetHistBuckets(),
		r.GetHistCounts(),
		r.GetService(),
		r.GetHost(),
		r.GetEnvironment(),
		r.GetK8SNamespace(),
		r.GetHttpMethod(),
		uint16(r.GetHttpStatusCode()), //nolint:gosec
		r.GetResource(),
		r.GetAttributes(),
	}
}
