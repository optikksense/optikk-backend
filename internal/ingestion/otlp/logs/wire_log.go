package logs

import (
	"encoding/json"
	"math"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

// WireLog mirrors the logs API JSON shape (internal/modules/logs/internal/shared.Log).
type WireLog struct {
	ID                string             `json:"id"`
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observed_timestamp"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number"`
	Body              string             `json:"body"`
	TraceID           string             `json:"trace_id"`
	SpanID            string             `json:"span_id"`
	TraceFlags        uint32             `json:"trace_flags"`
	ServiceName       string             `json:"service_name"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributes_string,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributes_number,omitempty"`
	AttributesBool    map[string]bool    `json:"attributes_bool,omitempty"`
	ScopeName         string             `json:"scope_name"`
	ScopeVersion      string             `json:"scope_version"`

	// Derived fields for historical semantic alignment (frontend expects these)
	Level   string `json:"level"`
	Message string `json:"message"`
	Service string `json:"service"`

	// EmitMs is wall-clock ms on the ingest server when this row was processed
	EmitMs int64 `json:"emit_ms"`
}

func wireLogFromIngestRow(row ingest.Row) (WireLog, bool) {
	return wireLogFromValues(row.Values)
}

func wireLogFromValues(values []any) (WireLog, bool) {
	if len(values) < 19 {
		return WireLog{}, false
	}

	ts, ok := asWireUint64(values[2])
	if !ok {
		return WireLog{}, false
	}
	observed, ok := asWireUint64(values[3])
	if !ok {
		return WireLog{}, false
	}
	id, ok := values[4].(string)
	if !ok {
		return WireLog{}, false
	}
	traceID, _ := values[5].(string)
	spanID, _ := values[6].(string)
	traceFlags, _ := asWireUint32(values[7])
	sevText, _ := values[8].(string)
	sevNum, _ := asWireUint8(values[9])
	body, _ := values[10].(string)

	attrStr := wireMapStringString(values[11])
	attrNum := wireMapStringFloat(values[12])
	attrBool := wireMapStringBool(values[13])
	res := wireMapStringAny(values[14])

	scopeName, _ := values[16].(string)
	scopeVer, _ := values[17].(string)

	service, host, pod, container, env := wireFlatResource(res)

	return WireLog{
		ID:                id,
		Timestamp:         ts,
		ObservedTimestamp: observed,
		SeverityText:      sevText,
		SeverityNumber:    sevNum,
		Body:              body,
		TraceID:           traceID,
		SpanID:            spanID,
		TraceFlags:        traceFlags,
		ServiceName:       service,
		Host:              host,
		Pod:               pod,
		Container:         container,
		Environment:       env,
		AttributesString:  attrStr,
		AttributesNumber:  attrNum,
		AttributesBool:    attrBool,
		ScopeName:         scopeName,
		ScopeVersion:      scopeVer,
		Level:             sevText, // Align with UI Level column
		Message:           body,    // Align with UI Message column
		Service:           service, // Align with UI Service column
	}, true
}

// LiveTailStreamPayload builds a WireLog object for livetail:log:streaming.
func LiveTailStreamPayload(row ingest.Row) (any, bool) {
	w, ok := wireLogFromIngestRow(row)
	if !ok {
		return nil, false
	}
	w.EmitMs = time.Now().UnixMilli()
	return w, true
}

func wireLogJSON(row ingest.Row) ([]byte, bool) {
	w, ok := wireLogFromIngestRow(row)
	if !ok {
		return nil, false
	}
	w.EmitMs = time.Now().UnixMilli()
	b, err := json.Marshal(w)
	if err != nil {
		return nil, false
	}
	return b, true
}

func wireFlatResource(res map[string]string) (service, host, pod, container, env string) {
	if res == nil {
		return "", "", "", "", ""
	}
	// Permissive check handles both dot-notation and flat keys
	service = res["service.name"]
	if service == "" {
		service = res["service_name"]
	}
	if service == "" {
		service = res["service"]
	}

	host = res["host.name"]
	if host == "" {
		host = res["host_name"]
	}
	if host == "" {
		host = res["host"]
	}

	return service, host, res["k8s.pod.name"], res["k8s.container.name"], res["deployment.environment"]
}

func wireMapStringAny(v any) map[string]string {
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		res := make(map[string]string, len(m))
		for k, val := range m {
			if s, ok := val.(string); ok {
				res[k] = s
			} else {
				res[k] = "" // Use better string conversion if available
			}
		}
		return res
	default:
		return nil
	}
}

func wireMapStringString(v any) map[string]string {
	return wireMapStringAny(v)
}


func wireMapStringFloat(v any) map[string]float64 {
	switch m := v.(type) {
	case map[string]float64:
		return m
	default:
		return nil
	}
}

func wireMapStringBool(v any) map[string]bool {
	switch m := v.(type) {
	case map[string]bool:
		return m
	default:
		return nil
	}
}

func asWireUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case time.Time:
		return uint64(x.UnixNano()), true
	case uint64:
		return x, true
	case uint32:
		return uint64(x), true
	case int64:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	default:
		return 0, false
	}
}

func asWireUint32(v any) (uint32, bool) {
	switch x := v.(type) {
	case uint32:
		return x, true
	case uint64:
		if x > math.MaxUint32 {
			return 0, false
		}
		return uint32(x), true //nolint:gosec // G115
	default:
		return 0, false
	}
}

func asWireUint8(v any) (uint8, bool) {
	switch x := v.(type) {
	case uint8:
		return x, true
	case uint32:
		if x > math.MaxUint8 {
			return 0, false
		}
		return uint8(x), true //nolint:gosec // G115
	default:
		return 0, false
	}
}
