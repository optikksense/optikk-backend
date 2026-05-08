package detail

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

// Service is the unified trace-detail surface — every per-trace and per-span
// read for the FE detail page. The FE fans these out client-side; the server
// does not compose them.
type Service interface {
	GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error)
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)
	ListSpanSubtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error)
}

type DetailService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &DetailService{repo: repo}
}

func (s *DetailService) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error) {
	row, err := s.repo.GetTraceSummary(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "detail: GetTraceSummary failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	out := mapTraceSummary(*row)
	return &out, nil
}

func (s *DetailService) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error) {
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

func (s *DetailService) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error) {
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
	resourceAttrs := map[string]string{} // spans table has no separate resource-attribute store

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

func (s *DetailService) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(ctx, teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}

func (s *DetailService) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
	rows, err := s.repo.ListSpansByTrace(ctx, teamID, traceID)
	if err != nil {
		return nil, err
	}
	fillStartNs(rows)
	return rows, nil
}

func (s *DetailService) ListSpanSubtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error) {
	rows, err := s.repo.ListSpanSubtree(ctx, teamID, spanID)
	if err != nil {
		return nil, err
	}
	subtree := filterSubtree(rows, spanID)
	fillStartNs(subtree)
	return subtree, nil
}

// fillStartNs derives the wire-level StartNs from the natively-scanned
// Timestamp column. The SQL reads `timestamp` (DateTime64) directly per the
// "scan native CH types; convert in Go" rule.
func fillStartNs(items []SpanListItem) {
	for i := range items {
		items[i].StartNs = items[i].Timestamp.UnixNano()
	}
}

func mapTraceSummary(r traceSummaryRow) TraceSummary {
	return TraceSummary{
		TraceID:        r.TraceID,
		StartMs:        uint64(r.StartTime.UnixMilli()), //nolint:gosec // G115
		EndMs:          uint64(r.EndTime.UnixMilli()),   //nolint:gosec // G115
		DurationMs:     float64(r.DurationNs) / 1_000_000,
		RootService:    r.RootService,
		RootOperation:  r.RootOperation,
		RootStatus:     r.RootStatus,
		RootHTTPMethod: r.RootHTTPMethod,
		RootHTTPStatus: r.RootHTTPStatus,
		SpanCount:      r.SpanCount,
		HasError:       r.HasError,
		ErrorCount:     r.ErrorCount,
		ServiceSet:     r.ServiceSet,
		Truncated:      r.Truncated,
	}
}

// flattenJSONAttrs mirrors the shape that CH's CAST(JSON, 'Map(String, String)')
// produces: every leaf value becomes a single map entry, keyed by the dot-joined
// path, with the value stringified. Returns nil for empty / malformed input.
//   - nested objects: keys joined with '.'
//   - arrays: element keys "<path>.<index>"; arrays of objects/arrays recurse
//   - numbers: shortest-round-trip repr (json.Number → string as written)
//   - bools: "true" / "false"
//   - null: emitted as ""
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

// splitEventRows folds the combined per-span result into separate event-row
// and exception-row lists. Each span contributes one event-row per element
// in its events array; spans with a non-empty exception_type contribute one
// exception-row each. Exceptions are reversed for display order (latest
// first).
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

// filterSubtree walks the flat list and keeps only spans reachable from
// rootID via the parent_span_id → span_id chain. Service-side; the CH query
// can't express transitive closure cleanly without recursive CTEs.
func filterSubtree(rows []SpanListItem, rootID string) []SpanListItem {
	keep := map[string]bool{rootID: true}
	changed := true
	for changed {
		changed = false
		for _, row := range rows {
			if !keep[row.SpanID] && keep[row.ParentSpanID] {
				keep[row.SpanID] = true
				changed = true
			}
		}
	}
	out := make([]SpanListItem, 0, len(rows))
	for _, row := range rows {
		if keep[row.SpanID] {
			out = append(out, row)
		}
	}
	return out
}
