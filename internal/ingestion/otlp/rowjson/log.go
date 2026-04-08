package rowjson

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

// MarshalLogRow encodes ingest row values as JSON for in-memory buffer payloads.
func MarshalLogRow(row ingest.Row) ([]byte, error) {
	return json.Marshal(row.Values)
}

// UnmarshalLogRow decodes JSON back to an ingest row for ClickHouse.
func UnmarshalLogRow(data []byte) (ingest.Row, error) {
	var raw []interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ingest.Row{}, err
	}
	v, err := normalizeLogValues(raw)
	if err != nil {
		return ingest.Row{}, err
	}
	return ingest.Row{Values: v}, nil
}

func normalizeLogValues(raw []interface{}) ([]any, error) {
	if len(raw) != 19 {
		return nil, fmt.Errorf("log row: expected 19 values, got %d", len(raw))
	}
	out := make([]any, 19)
	out[0] = toUint32(raw[0])
	out[1] = toUint32(raw[1])
	out[2] = toUint64(raw[2])
	out[3] = toUint64(raw[3])
	out[4] = toString(raw[4])
	out[5] = toString(raw[5])
	out[6] = toString(raw[6])
	out[7] = toUint32(raw[7])
	out[8] = toString(raw[8])
	out[9] = toUint8(raw[9])
	out[10] = toString(raw[10])
	out[11] = toStringMap(raw[11])
	out[12] = toFloat64Map(raw[12])
	out[13] = toBoolMap(raw[13])
	out[14] = toStringMap(raw[14])
	out[15] = toString(raw[15])
	out[16] = toString(raw[16])
	out[17] = toString(raw[17])
	out[18] = toStringMap(raw[18])
	return out, nil
}

func toUint32(v interface{}) uint32 {
	switch x := v.(type) {
	case float64:
		if x < 0 || x > math.MaxUint32 {
			return 0
		}
		return uint32(x) //nolint:gosec // G115
	case json.Number:
		n, _ := x.Int64()
		if n < 0 || n > math.MaxUint32 {
			return 0
		}
		return uint32(n) //nolint:gosec // G115
	case uint32:
		return x
	case uint64:
		if x > math.MaxUint32 {
			return 0
		}
		return uint32(x) //nolint:gosec // G115
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch x := v.(type) {
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x) //nolint:gosec // G115
	case json.Number:
		n, _ := x.Int64()
		if n < 0 {
			return 0
		}
		return uint64(n) //nolint:gosec // G115
	case uint64:
		return x
	case uint32:
		return uint64(x)
	default:
		return 0
	}
}

func toUint8(v interface{}) uint8 {
	switch x := v.(type) {
	case float64:
		if x < 0 || x > math.MaxUint8 {
			return 0
		}
		return uint8(x) //nolint:gosec // G115
	case json.Number:
		n, _ := x.Int64()
		if n < 0 || n > math.MaxUint8 {
			return 0
		}
		return uint8(n) //nolint:gosec // G115
	case uint8:
		return x
	default:
		return 0
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

func toStringMap(v interface{}) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]interface{}:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = toString(val)
		}
		return out
	default:
		return nil
	}
}

func toFloat64Map(v interface{}) map[string]float64 {
	switch m := v.(type) {
	case map[string]float64:
		return m
	case map[string]interface{}:
		out := make(map[string]float64, len(m))
		for k, val := range m {
			switch n := val.(type) {
			case float64:
				out[k] = n
			case json.Number:
				f, _ := n.Float64()
				out[k] = f
			}
		}
		return out
	default:
		return nil
	}
}

func toBoolMap(v interface{}) map[string]bool {
	switch m := v.(type) {
	case map[string]bool:
		return m
	case map[string]interface{}:
		out := make(map[string]bool, len(m))
		for k, val := range m {
			switch b := val.(type) {
			case bool:
				out[k] = b
			case float64:
				out[k] = b != 0
			}
		}
		return out
	default:
		return nil
	}
}
