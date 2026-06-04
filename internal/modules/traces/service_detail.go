package traces

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

<<<<<<< HEAD:internal/modules/traces/service_detail.go
func (s *TracesService) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error) {
=======
type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/service.go
	row, err := s.repo.GetTraceSummary(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "detail: GetTraceSummary failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return row, nil
}

<<<<<<< HEAD:internal/modules/traces/service_detail.go
func (s *TracesService) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error) {
=======
func (s *Service) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/service.go
	combined, err := s.repo.GetSpanEvents(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "detail: GetSpanEvents failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
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

<<<<<<< HEAD:internal/modules/traces/service_detail.go
func (s *TracesService) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error) {
=======
func (s *Service) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/service.go
	row, err := s.repo.GetSpanAttributes(ctx, teamID, traceID, spanID)
	if err != nil {
		slog.ErrorContext(ctx, "detail: GetSpanAttributes failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID), slog.String("span_id", spanID))
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	attrs := flattenJSONAttrs(row.AttributesJSON)
	if attrs == nil {
		attrs = map[string]string{}
	}
	resourceAttrs := map[string]string{}

	return &SpanAttributes{
		SpanID:                row.SpanID,
		TraceID:               row.TraceID,
		OperationName:         row.OperationName,
		ServiceName:           row.ServiceName,
		AttributesString:      attrs,
		ResourceAttrs:         resourceAttrs,
		Attributes:            attrs,
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

<<<<<<< HEAD:internal/modules/traces/service_detail.go
func (s *TracesService) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(ctx, teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}

func (s *TracesService) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
=======
func (s *Service) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(ctx, teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}

func (s *Service) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/service.go
	rows, err := s.repo.ListSpansByTrace(ctx, teamID, traceID)
	if err != nil {
		return nil, err
	}
	fillStartNs(rows)
	return rows, nil
}

func fillStartNs(items []SpanListItem) {
	for i := range items {
		items[i].StartNs = items[i].Timestamp.UnixNano()
	}
}

func flattenJSONAttrs(jsonStr string) map[string]string {
	jsonStr = strings.TrimSpace(jsonStr)
	if jsonStr == "" || jsonStr == "{}" || jsonStr == "null" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil
	}
	out := map[string]string{}
	walkJSONAttrs(out, "", root)
	if len(out) == 0 {
		return nil
	}
	return out
}

func walkJSONAttrs(out map[string]string, prefix string, v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, vv := range x {
			next := k
			if prefix != "" {
				next = prefix + "." + k
			}
			walkJSONAttrs(out, next, vv)
		}
	case []any:
		for i, vv := range x {
			next := strconv.Itoa(i)
			if prefix != "" {
				next = prefix + "." + strconv.Itoa(i)
			}
			walkJSONAttrs(out, next, vv)
		}
	case nil:
		if prefix != "" {
			out[prefix] = ""
		}
	case string:
		if prefix != "" {
			out[prefix] = x
		}
	case bool:
		if prefix != "" {
			if x {
				out[prefix] = "true"
			} else {
				out[prefix] = "false"
			}
		}
	case json.Number:
		if prefix != "" {
			out[prefix] = x.String()
		}
	default:
		if prefix != "" {
			out[prefix] = fmt.Sprint(x)
		}
	}
}

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

var (
	reNumberLiteral = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	reStringLiteral = regexp.MustCompile(`'[^']*'`)
	reMultiSpace    = regexp.MustCompile(`\s+`)
)

func normalizeDBStatement(stmt string) string {
	if stmt == "" {
		return ""
	}
	s := reStringLiteral.ReplaceAllString(stmt, "?")
	s = reNumberLiteral.ReplaceAllString(s, "?")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

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
