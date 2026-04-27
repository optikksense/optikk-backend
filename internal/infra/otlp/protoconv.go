package otlp

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
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

// ResourceFingerprint computes a stable, order-independent xxhash64 of resource attributes.
func ResourceFingerprint(kvs []*commonpb.KeyValue) uint64 {
	if len(kvs) == 0 {
		return 0
	}

	// 1. Copy and Sort lexicographically by Key to guarantee order-independent hashing
	sorted := make([]*commonpb.KeyValue, len(kvs))
	copy(sorted, kvs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	// 2. Hash using xxhash with null-byte delimiters to prevent boundary collisions
	h := xxhash.New()
	for _, kv := range sorted {
		_, _ = h.Write([]byte(kv.Key))
		_, _ = h.Write([]byte{0}) // null delimiter
		_, _ = h.Write([]byte(AnyValueString(kv.Value)))
		_, _ = h.Write([]byte{0}) // null delimiter
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
