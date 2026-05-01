package tracedetail

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"sort"
	"strings"
)

type Service interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) (*TraceLogsResponse, error)
}

type TraceDetailService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &TraceDetailService{repo: repo}
}

func (s *TraceDetailService) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error) {
	combined, err := s.repo.GetSpanEvents(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "tracedetail: GetSpanEvents failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	eventRows, exceptionRows := splitEventRows(combined)

	events := make([]SpanEvent, 0, len(eventRows))
	seenException := make(map[string]bool, len(eventRows))
	for _, row := range eventRows {
		name, attrJSON := parseEventJSON(row.EventJSON)
		if name == "exception" {
			seenException[row.SpanID] = true
		}
		events = append(events, SpanEvent{
			SpanID:     row.SpanID,
			TraceID:    row.TraceID,
			EventName:  name,
			Timestamp:  row.Timestamp,
			Attributes: attrJSON,
		})
	}

	for _, row := range exceptionRows {
		if seenException[row.SpanID] {
			continue
		}
		attrs := map[string]string{}
		if row.ExceptionType != "" {
			attrs["exception.type"] = row.ExceptionType
		}
		if row.ExceptionMessage != "" {
			attrs["exception.message"] = row.ExceptionMessage
		}
		if row.ExceptionStacktrace != "" {
			attrs["exception.stacktrace"] = row.ExceptionStacktrace
		}
		attrJSON := "{}"
		if len(attrs) > 0 {
			if b, marshalErr := json.Marshal(attrs); marshalErr == nil {
				attrJSON = string(b)
			}
		}
		events = append(events, SpanEvent{
			SpanID:     row.SpanID,
			TraceID:    row.TraceID,
			EventName:  "exception",
			Timestamp:  row.Timestamp,
			Attributes: attrJSON,
		})
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].Timestamp.Equal(events[j].Timestamp) {
			if events[i].SpanID == events[j].SpanID {
				return events[i].EventName < events[j].EventName
			}
			return events[i].SpanID < events[j].SpanID
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
	return events, nil
}

func (s *TraceDetailService) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error) {
	row, err := s.repo.GetSpanAttributes(ctx, teamID, traceID, spanID)
	if err != nil {
		slog.ErrorContext(ctx, "tracedetail: GetSpanAttributes failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID), slog.String("span_id", spanID))
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	merged := make(map[string]string, len(row.AttributesString)+len(row.ResourceAttrs))
	for k, v := range row.ResourceAttrs {
		merged[k] = v
	}
	for k, v := range row.AttributesString {
		merged[k] = v
	}

	return &SpanAttributes{
		SpanID:                row.SpanID,
		TraceID:               row.TraceID,
		OperationName:         row.OperationName,
		ServiceName:           row.ServiceName,
		AttributesString:      row.AttributesString,
		ResourceAttrs:         row.ResourceAttrs,
		Attributes:            merged,
		ExceptionType:         row.ExceptionType,
		ExceptionMessage:      row.ExceptionMessage,
		ExceptionStacktrace:   row.ExceptionStacktrace,
		DBSystem:              row.DBSystem,
		DBName:                row.DBName,
		DBStatement:           row.DBStatement,
		DBStatementNormalized: normalizeDBStatement(row.DBStatement),
		Links:                 parseSpanLinks(row.Links),
	}, nil
}

func (s *TraceDetailService) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(ctx, teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}

// splitEventRows folds the combined per-span result into separate event-row
// and exception-row lists. Each span contributes one event-row per element
// in its events array; spans with a non-empty exception_type contribute one
// exception-row each. Exceptions are reversed for display order (latest
// first). Moved here from the repository — pure transformation, no DB.
func splitEventRows(rows []spanEventCombinedRow) ([]spanEventRow, []exceptionRow) {
	var events []spanEventRow
	var exceptions []exceptionRow
	for _, r := range rows {
		for _, ev := range r.Events {
			events = append(events, spanEventRow{
				SpanID: r.SpanID, TraceID: r.TraceID, Timestamp: r.Timestamp, EventJSON: ev,
			})
		}
		if r.ExceptionType != "" {
			exceptions = append(exceptions, exceptionRow{
				SpanID: r.SpanID, TraceID: r.TraceID, Timestamp: r.Timestamp,
				ExceptionType: r.ExceptionType, ExceptionMessage: r.ExceptionMessage,
				ExceptionStacktrace: r.ExceptionStacktrace,
			})
		}
	}
	for i, j := 0, len(exceptions)-1; i < j; i, j = i+1, j-1 {
		exceptions[i], exceptions[j] = exceptions[j], exceptions[i]
	}
	return events, exceptions
}

// db-statement normalization moved here from the repository — pure
// text transformation that belongs in service.
var (
	reNumberLiteral = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	reStringLiteral = regexp.MustCompile(`'[^']*'`)
	reMultiSpace    = regexp.MustCompile(`\s+`)
)

// normalizeDBStatement collapses literals to `?` placeholders so similar
// db.statement values group together in the UI.
func normalizeDBStatement(stmt string) string {
	if stmt == "" {
		return ""
	}
	s := reStringLiteral.ReplaceAllString(stmt, "?")
	s = reNumberLiteral.ReplaceAllString(s, "?")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// parseEventJSON extracts the event name and attributes JSON from an event
// string stored in the events column. New format events are JSON objects:
//
//	{"name":"...","timeUnixNano":"...","attributes":{...}}
//
// Legacy events are plain strings containing just the event name.
func parseEventJSON(raw string) (name string, attrs string) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 {
		return "", "{}"
	}
	if raw[0] != '{' {
		return raw, "{}"
	}
	var obj struct {
		Name       string            `json:"name"`
		Attributes map[string]string `json:"attributes"`
	}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw, "{}"
	}
	if len(obj.Attributes) == 0 {
		return obj.Name, "{}"
	}
	b, err := json.Marshal(obj.Attributes)
	if err != nil {
		return obj.Name, "{}"
	}
	return obj.Name, string(b)
}
