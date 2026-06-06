package spans

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
)

const chTable = "observability.spans"

// chColumns mirrors the column order in db/clickhouse/01_spans.sql.
var chColumns = []string{
	"ts_bucket", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host",
	"response_status_code",
	"service", "host", "pod", "service_version", "environment",
	"peer_service", "db_system", "db_name", "db_statement", "http_route",
	"http_status_bucket",
	"attributes",
	"fingerprint",
	"events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

// Writer batch-inserts decoded Rows into ClickHouse using async inserts.
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
		return fmt.Errorf("spans writer: prepare: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(rowValues(r)...); err != nil {
			return fmt.Errorf("spans writer: append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("spans writer: send: %w", err)
	}
	return nil
}

// rowValues returns the positional argument slice aligned with chColumns.
func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTsBucket(),
		r.GetTeamId(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetParentSpanId(),
		r.GetTraceState(),
		r.GetFlags(),
		r.GetName(),
		int8(r.GetKind()),
		r.GetKindString(),
		r.GetDurationNano(),
		r.GetHasError(),
		int16(r.GetStatusCode()),
		r.GetStatusCodeString(),
		r.GetStatusMessage(),
		r.GetHttpUrl(),
		r.GetHttpMethod(),
		r.GetHttpHost(),
		r.GetResponseStatusCode(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetServiceVersion(),
		r.GetEnvironment(),
		r.GetPeerService(),
		r.GetDbSystem(),
		r.GetDbName(),
		r.GetDbStatement(),
		r.GetHttpRoute(),
		r.GetHttpStatusBucket(),
		r.GetAttributes(),
		r.GetFingerprint(),
		r.GetEvents(),
		r.GetLinks(),
		r.GetExceptionType(),
		r.GetExceptionMessage(),
		r.GetExceptionStacktrace(),
		r.GetExceptionEscaped(),
	}
}
