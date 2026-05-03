package tracedetail

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
)

// GetSpanLogs backs /traces/:traceId/spans/:spanId/logs (O8). Uses the
// existing log row shape so the drawer can reuse the trace-level logs columns.
func (s *TraceDetailService) GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) (*TraceLogsResponse, error) {
	rows, err := s.repo.GetSpanLogs(ctx, teamID, traceID, spanID)
	if err != nil {
		slog.ErrorContext(ctx, "tracedetail: GetSpanLogs failed", slog.Any("error", err),
			slog.Int64("team_id", teamID), slog.String("trace_id", traceID), slog.String("span_id", spanID))
		return nil, err
	}
	return &TraceLogsResponse{Logs: mapTraceLogs(rows), IsSpeculative: false}, nil
}

// mapTraceLogs converts raw rows into the shared TraceLog wire model. Kept
// alongside GetSpanLogs so the original trace-logs path can stay self-contained.
func mapTraceLogs(rows []traceLogRow) []TraceLog {
	logs := make([]TraceLog, len(rows))
	for i, row := range rows {
		logs[i] = TraceLog{
			Timestamp:         uint64(row.Timestamp.UnixNano()), //nolint:gosec
			ObservedTimestamp: row.ObservedTimestamp,
			SeverityText:      row.SeverityText,
			SeverityNumber:    row.SeverityNumber,
			Body:              row.Body,
			TraceID:           row.TraceID,
			SpanID:            row.SpanID,
			TraceFlags:        row.TraceFlags,
			ServiceName:       row.ServiceName,
			Host:              row.Host,
			Pod:               row.Pod,
			Container:         row.Container,
			Environment:       row.Environment,
			AttributesString:  row.AttributesString,
			AttributesNumber:  row.AttributesNumber,
			AttributesBool:    row.AttributesBool,
			ScopeName:         row.ScopeName,
			ScopeVersion:      row.ScopeVersion,
		}
	}
	return logs
}

// parseSpanLinks decodes the `links` column (JSON array of OTLP link objects)
// into typed SpanLink records. Returns nil on empty/invalid input — link data
// is optional, never fatal.
func parseSpanLinks(raw string) []SpanLink {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" {
		return nil
	}
	var wire []spanLinkWire
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil
	}
	out := make([]SpanLink, 0, len(wire))
	for _, l := range wire {
		out = append(out, SpanLink{
			TraceID:    l.TraceID,
			SpanID:     l.SpanID,
			TraceState: l.TraceState,
			Attributes: l.Attributes,
		})
	}
	return out
}

type spanLinkWire struct {
	TraceID    string            `json:"traceId"`
	SpanID     string            `json:"spanId"`
	TraceState string            `json:"traceState,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}
