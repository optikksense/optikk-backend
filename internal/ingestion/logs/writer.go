package logs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
)

const chTable = "observability.logs"

var chColumns = []string{
	"team_id", "ts_bucket", "timestamp", "observed_timestamp",
	"trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "fingerprint",
	"scope_name", "scope_version",
	"service", "host", "pod", "container", "environment",
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
		return fmt.Errorf("logs writer: prepare: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(rowValues(r)...); err != nil {
			return fmt.Errorf("logs writer: append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("logs writer: send: %w", err)
	}
	return nil
}

func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetTsBucket(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetObservedTimestampNs(),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetTraceFlags(),
		r.GetSeverityText(),
		uint8(r.GetSeverityNumber()), //nolint:gosec
		r.GetBody(),
		r.GetAttributesString(),
		r.GetAttributesNumber(),
		r.GetAttributesBool(),
		r.GetResource(),
		r.GetFingerprint(),
		r.GetScopeName(),
		r.GetScopeVersion(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetEnvironment(),
	}
}
