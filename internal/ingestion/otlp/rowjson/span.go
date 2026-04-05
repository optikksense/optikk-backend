package rowjson

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

const spanValueCount = 30

// MarshalSpanRow encodes span ingest row values as JSON.
func MarshalSpanRow(row ingest.Row) ([]byte, error) {
	return json.Marshal(row.Values)
}

// UnmarshalSpanRow decodes JSON back to a span ingest row.
func UnmarshalSpanRow(data []byte) (ingest.Row, error) {
	var raw []interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ingest.Row{}, err
	}
	v, err := normalizeSpanValues(raw)
	if err != nil {
		return ingest.Row{}, err
	}
	return ingest.Row{Values: v}, nil
}

func normalizeSpanValues(raw []interface{}) ([]any, error) {
	if len(raw) != spanValueCount {
		return nil, fmt.Errorf("span row: expected %d values, got %d", spanValueCount, len(raw))
	}
	out := make([]any, spanValueCount)
	out[0] = toUint64(raw[0]) // ts_bucket_start
	out[1] = toUint32(raw[1])
	ts, err := toTime(raw[2])
	if err != nil {
		return nil, err
	}
	out[2] = ts
	out[3] = toString(raw[3])
	out[4] = toString(raw[4])
	out[5] = toString(raw[5])
	out[6] = toString(raw[6])
	out[7] = toUint32(raw[7])
	out[8] = toString(raw[8])
	out[9] = toInt8(raw[9])
	out[10] = toString(raw[10])
	out[11] = toUint64(raw[11])
	out[12] = toBool(raw[12])
	out[13] = toBool(raw[13])
	out[14] = toInt16(raw[14])
	out[15] = toString(raw[15])
	out[16] = toString(raw[16])
	out[17] = toString(raw[17])
	out[18] = toString(raw[18])
	out[19] = toString(raw[19])
	out[20] = toString(raw[20])
	out[21] = toString(raw[21])
	out[22] = toString(raw[22])
	out[23] = toStringMap(raw[23])
	out[24] = toStringSlice(raw[24]) // events: ClickHouse Array(String)
	out[25] = toString(raw[25])
	out[26] = toString(raw[26])
	out[27] = toString(raw[27])
	out[28] = toString(raw[28])
	out[29] = toBool(raw[29])
	return out, nil
}

func toInt8(v interface{}) int8 {
	switch x := v.(type) {
	case float64:
		if x < math.MinInt8 || x > math.MaxInt8 {
			return 0
		}
		return int8(x) //nolint:gosec // G115
	case json.Number:
		n, _ := x.Int64()
		if n < math.MinInt8 || n > math.MaxInt8 {
			return 0
		}
		return int8(n) //nolint:gosec // G115
	case int8:
		return x
	default:
		return 0
	}
}

func toInt16(v interface{}) int16 {
	switch x := v.(type) {
	case float64:
		if x < math.MinInt16 || x > math.MaxInt16 {
			return 0
		}
		return int16(x) //nolint:gosec // G115
	case json.Number:
		n, _ := x.Int64()
		if n < math.MinInt16 || n > math.MaxInt16 {
			return 0
		}
		return int16(n) //nolint:gosec // G115
	case int16:
		return x
	default:
		return 0
	}
}

// toStringSlice decodes JSON arrays to []string for ClickHouse Array(String) (e.g. span events).
func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case []interface{}:
		out := make([]string, 0, len(s))
		for _, e := range s {
			out = append(out, toString(e))
		}
		return out
	default:
		return nil
	}
}
