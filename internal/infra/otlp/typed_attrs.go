package otlp

import (
	"sort"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// TypedAttrs partitions OTLP KeyValue attributes into three typed maps in one
// pass (strings, numbers, bools) and caps the string map at maxStringKeys
// using a deterministic sort-by-key truncation. Number and bool maps are not
// capped — in practice they stay tiny (<16 entries) and dropping them
// silently hides signal.
//
// Bytes values are treated as strings (hex-decoded upstream) because the
// attribute-string CH column is `Map(String,String)`; no Map(String,Bytes).
//
// Returned maps are freshly allocated — if the caller wants pooled maps,
// pass them in via TypedAttrsInto instead.
func TypedAttrs(kvs []*commonpb.KeyValue, maxStringKeys int) (
	strMap map[string]string,
	numMap map[string]float64,
	boolMap map[string]bool,
	dropped int,
) {
	strMap = make(map[string]string, len(kvs))
	numMap = make(map[string]float64)
	boolMap = make(map[string]bool)
	dropped = TypedAttrsInto(kvs, maxStringKeys, strMap, numMap, boolMap)
	return
}

// TypedAttrsInto is the pooled-map variant of TypedAttrs. Returns the count
// of string-map entries dropped by the cap.
func TypedAttrsInto(
	kvs []*commonpb.KeyValue,
	maxStringKeys int,
	strMap map[string]string,
	numMap map[string]float64,
	boolMap map[string]bool,
) int {
	for _, kv := range kvs {
		if kv == nil || kv.Value == nil {
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
	if maxStringKeys > 0 && len(strMap) > maxStringKeys {
		return capStringMapDeterministic(strMap, maxStringKeys)
	}
	return 0
}

// capStringMapDeterministic trims strMap down to exactly max entries, keeping
// the lexicographically-smallest keys so different Go runtimes + map
// iteration orders produce the same set. Returns the number of entries that
// were dropped.
func capStringMapDeterministic(strMap map[string]string, max int) int {
	return CapStringMap(strMap, max)
}

// CapStringMap trims strMap down to exactly max entries, keeping the
// lexicographically-smallest keys. Shared by signals that carry a single
// merged attribute map (spans) alongside those using the typed 3-way split
// (logs). Returns the count of entries dropped; ≤0 means no-op.
func CapStringMap(strMap map[string]string, max int) int {
	if max <= 0 || len(strMap) <= max {
		return 0
	}
	keys := make([]string, 0, len(strMap))
	for k := range strMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	dropped := 0
	for _, k := range keys[max:] {
		delete(strMap, k)
		dropped++
	}
	return dropped
}
