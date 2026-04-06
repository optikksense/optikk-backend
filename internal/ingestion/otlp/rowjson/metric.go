package rowjson

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

// MarshalMetricRow encodes metric ingest row values as JSON.
func MarshalMetricRow(row ingest.Row) ([]byte, error) {
	return json.Marshal(row.Values)
}

// UnmarshalMetricRow decodes JSON back to a metric ingest row.
func UnmarshalMetricRow(data []byte) (ingest.Row, error) {
	var raw []interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ingest.Row{}, err
	}
	v, err := normalizeMetricValues(raw)
	if err != nil {
		return ingest.Row{}, err
	}
	return ingest.Row{Values: v}, nil
}

func normalizeMetricValues(raw []interface{}) ([]any, error) {
	if len(raw) != 16 {
		return nil, fmt.Errorf("metric row: expected 16 values, got %d", len(raw))
	}
	out := make([]any, 16)
	out[0] = toUint32(raw[0])
	out[1] = toString(raw[1])
	out[2] = toString(raw[2])
	out[3] = toString(raw[3])
	out[4] = toString(raw[4])
	out[5] = toBool(raw[5])
	out[6] = toString(raw[6])
	out[7] = toString(raw[7])
	out[8] = toUint64(raw[8])
	ts, err := toTime(raw[9])
	if err != nil {
		return nil, err
	}
	out[9] = ts
	out[10] = toFloat64(raw[10])
	out[11] = toFloat64(raw[11])
	out[12] = toUint64(raw[12])
	out[13] = toFloat64Slice(raw[13])
	out[14] = toUint64Slice(raw[14])
	out[15] = toJSONAttributes(raw[15])
	return out, nil
}

func toBool(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	default:
		return false
	}
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

func toTime(v interface{}) (time.Time, error) {
	switch x := v.(type) {
	case string:
		return time.Parse(time.RFC3339Nano, x)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return time.Time{}, err
		}
		var t time.Time
		if err := json.Unmarshal(b, &t); err != nil {
			return time.Time{}, err
		}
		return t, nil
	}
}

func toFloat64Slice(v interface{}) []float64 {
	switch s := v.(type) {
	case []float64:
		return s
	case []interface{}:
		out := make([]float64, 0, len(s))
		for _, e := range s {
			out = append(out, toFloat64(e))
		}
		return out
	default:
		return nil
	}
}

func toUint64Slice(v interface{}) []uint64 {
	switch s := v.(type) {
	case []uint64:
		return s
	case []interface{}:
		out := make([]uint64, 0, len(s))
		for _, e := range s {
			out = append(out, toUint64(e))
		}
		return out
	default:
		return nil
	}
}

// toJSONAttributes preserves attributes as map for ClickHouse JSON column.
func toJSONAttributes(v interface{}) interface{} {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]interface{}:
		return m
	default:
		return v
	}
}
