package logs

import (
	"log/slog"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

const maxLogAttributes = 128

// typedAttrs partitions OTLP KeyValues into the three typed maps the CH schema
// expects: strings, numbers (int + double), bools. Bytes are treated as strings.
func typedAttrs(kvs []*commonpb.KeyValue) (strMap map[string]string, numMap map[string]float64, boolMap map[string]bool) {
	strMap = make(map[string]string, len(kvs))
	numMap = make(map[string]float64)
	boolMap = make(map[string]bool)
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		switch val := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			strMap[kv.Key] = val.StringValue
		case *commonpb.AnyValue_IntValue:
			numMap[kv.Key] = float64(val.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			numMap[kv.Key] = val.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			boolMap[kv.Key] = val.BoolValue
		case *commonpb.AnyValue_BytesValue:
			strMap[kv.Key] = string(val.BytesValue)
		}
	}
	return strMap, numMap, boolMap
}

// capStringAttrs truncates string attributes to maxLogAttributes, warning once
// per over-limit record. Number and bool maps are not capped — in practice
// they are tiny.
func capStringAttrs(attrs map[string]string, teamID int64) map[string]string {
	if len(attrs) <= maxLogAttributes {
		return attrs
	}
	slog.Warn("ingest: log attributes truncated",
		slog.Int("from", len(attrs)), slog.Int("to", maxLogAttributes),
		slog.Int64("team_id", teamID))
	trimmed := make(map[string]string, maxLogAttributes)
	i := 0
	for k, v := range attrs {
		trimmed[k] = v
		i++
		if i == maxLogAttributes {
			break
		}
	}
	return trimmed
}
