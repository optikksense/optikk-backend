package protoconv

import (
	"encoding/hex"
	"hash/fnv"
	"strconv"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// AnyValueString converts a proto AnyValue to its string representation.
func AnyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		if val.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_BytesValue:
		return string(val.BytesValue)
	default:
		return ""
	}
}

// AttrsToMap converts proto KeyValue pairs to a string map.
func AttrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = AnyValueString(kv.Value)
	}
	return m
}

// MergeAttrsMap merges a resource map with proto datapoint attributes.
func MergeAttrsMap(resMap map[string]string, dpAttrs []*commonpb.KeyValue) map[string]string {
	if len(dpAttrs) == 0 {
		return resMap
	}
	merged := make(map[string]string, len(resMap)+len(dpAttrs))
	for k, v := range resMap {
		merged[k] = v
	}
	for _, kv := range dpAttrs {
		merged[kv.Key] = AnyValueString(kv.Value)
	}
	return merged
}

// ResourceFingerprint computes a stable FNV-64a hash of resource attributes.
func ResourceFingerprint(kvs []*commonpb.KeyValue) uint64 {
	h := fnv.New64a()
	for _, kv := range kvs {
		_, _ = h.Write([]byte(kv.Key))
		_, _ = h.Write([]byte(AnyValueString(kv.Value)))
	}
	return h.Sum64()
}

// NanoToTime converts a nanosecond timestamp to time.Time, defaulting to now.
func NanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now()
	}
	return time.Unix(0, int64(ns)) //nolint:gosec // G115
}

// BytesToHex converts a byte slice to a hex string.
func BytesToHex(b []byte) string {
	if len(b) > 0 {
		return hex.EncodeToString(b)
	}
	return ""
}
