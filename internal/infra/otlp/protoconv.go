// Package otlp hosts OTLP-to-local conversion helpers shared by the per-signal
// ingest mappers. It has no Kafka or ClickHouse knowledge — pure translation
// from the upstream protobuf types into primitive Go values.
package otlp

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func AnyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	return pcommonValueToString(v)
}

func pcommonValueToString(v *commonpb.AnyValue) string {
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	default:
		// For maps and arrays, we use the pdata-like string representation
		return fmt.Sprintf("%v", v.Value)
	}
}

func AttrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = AnyValueString(kv.Value)
	}
	return m
}

func MergeAttrsMap(resMap map[string]string, dpAttrs []*commonpb.KeyValue) map[string]string {
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

// NanoToTime converts a nanosecond timestamp to time.Time, defaulting to now().
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
