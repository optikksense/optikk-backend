package spans

import (
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

// WireSpan matches traces/livetail.LiveSpan JSON shape plus emit_ms.
type WireSpan struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	DurationMs    float64   `json:"durationMs"`
	Status        string    `json:"status"`
	Host          string    `json:"host,omitempty"`
	HTTPMethod    string    `json:"httpMethod,omitempty"`
	HTTPStatus    string    `json:"httpStatusCode,omitempty"`
	SpanKind      string    `json:"spanKind,omitempty"`
	HasError      bool      `json:"hasError"`
	Timestamp     time.Time `json:"timestamp"`
	EmitMs        int64     `json:"emit_ms"`
}

// SpanLiveTailStreamPayload encodes an ingest row for span snapshots or broadcasting.
func SpanLiveTailStreamPayload(row ingest.Row, emitMs int64) (any, error) {
	w, err := liveSpanTailFromRowValues(row.Values)
	if err != nil {
		return nil, err
	}
	w.EmitMs = emitMs
	return w, nil
}

func liveSpanTailFromRowValues(v []any) (WireSpan, error) {
	if len(v) < 30 {
		return WireSpan{}, fmt.Errorf("span row: short values")
	}
	ts, ok := v[2].(time.Time)
	if !ok {
		return WireSpan{}, fmt.Errorf("span row: bad timestamp")
	}
	traceID := fmt.Sprint(v[3])
	spanID := fmt.Sprint(v[4])
	name := fmt.Sprint(v[8])
	kindStr := fmt.Sprint(v[10])
	durNano, _ := toWireUint64(v[11])
	statusStr := fmt.Sprint(v[15])
	httpMethod := fmt.Sprint(v[18])
	httpStatus := fmt.Sprint(v[22])
	hasErr, _ := v[12].(bool)

	res := wireMapStringAny(v[23])
	service, host, _, _, _ := wireFlatResource(res)

	return WireSpan{
		SpanID:        spanID,
		TraceID:       traceID,
		ServiceName:   service,
		Host:          host,
		OperationName: name,
		DurationMs:    float64(durNano) / 1e6,
		Status:        statusStr,
		HTTPMethod:    httpMethod,
		HTTPStatus:    httpStatus,
		SpanKind:      kindStr,
		HasError:      hasErr,
		Timestamp:     ts,
		EmitMs:        0, // placeholder, will be set by caller
	}, nil
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
				res[k] = ""
			}
		}
		return res
	default:
		return nil
	}
}

func toWireUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case float64:
		if x < 0 {
			return 0, true
		}
		return uint64(x), true //nolint:gosec // G115
	default:
		return 0, false
	}
}
