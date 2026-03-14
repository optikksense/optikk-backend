package tracedetail

import (
	"encoding/json"
	"sort"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

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
		// Legacy format: plain event name string.
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

func (r *ClickHouseRepository) GetSpanEvents(teamID int64, traceID string) ([]SpanEvent, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.timestamp, event_json
		FROM observability.spans s
		ARRAY JOIN s.events AS event_json
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	events := make([]SpanEvent, 0, len(rows))
	seenExceptionEvent := make(map[string]bool, len(rows))
	for _, row := range rows {
		spanID := dbutil.StringFromAny(row["span_id"])
		rawEvent := dbutil.StringFromAny(row["event_json"])
		eventName, attrJSON := parseEventJSON(rawEvent)
		if eventName == "exception" {
			seenExceptionEvent[spanID] = true
		}
		events = append(events, SpanEvent{
			SpanID:     spanID,
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			EventName:  eventName,
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
			Attributes: attrJSON,
		})
	}

	exceptionRows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.timestamp, s.exception_type, s.exception_message, s.exception_stacktrace
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		  AND (s.exception_type != '' OR s.exception_message != '' OR s.exception_stacktrace != '')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}
	for _, row := range exceptionRows {
		spanID := dbutil.StringFromAny(row["span_id"])
		if seenExceptionEvent[spanID] {
			continue
		}
		attrs := map[string]string{}
		if v := dbutil.StringFromAny(row["exception_type"]); v != "" {
			attrs["exception.type"] = v
		}
		if v := dbutil.StringFromAny(row["exception_message"]); v != "" {
			attrs["exception.message"] = v
		}
		if v := dbutil.StringFromAny(row["exception_stacktrace"]); v != "" {
			attrs["exception.stacktrace"] = v
		}
		attrJSON := "{}"
		if len(attrs) > 0 {
			if b, marshalErr := json.Marshal(attrs); marshalErr == nil {
				attrJSON = string(b)
			}
		}
		events = append(events, SpanEvent{
			SpanID:     spanID,
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			EventName:  "exception",
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
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
