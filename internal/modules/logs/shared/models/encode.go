package models

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// EncodeLogID builds the base64url(trace_id:span_id:ts_ns) deep-link id.
func EncodeLogID(d LogRow) string {
	raw := fmt.Sprintf("%s:%s:%d", d.TraceID, d.SpanID, d.Timestamp.UnixNano())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// ParseLogID decodes a base64url(trace_id:span_id:ts_ns) deep-link id.
func ParseLogID(id string) (traceID, spanID string, tsNs int64, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return "", "", 0, false
	}
	parts := strings.SplitN(string(raw), ":", 3)
	if len(parts) != 3 {
		return "", "", 0, false
	}
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return parts[0], parts[1], ts, true
}

// MapLog converts a CH scan row into the JSON wire model.
func MapLog(d LogRow) Log {
	return Log{
		ID:                EncodeLogID(d),
		Timestamp:         uint64(d.Timestamp.UnixNano()), //nolint:gosec // G115 domain-bounded
		ObservedTimestamp: d.ObservedTimestamp,
		SeverityText:      d.SeverityText,
		SeverityNumber:    d.SeverityNumber,
		SeverityBucket:    d.SeverityBucket,
		Body:              d.Body,
		TraceID:           d.TraceID,
		SpanID:            d.SpanID,
		TraceFlags:        d.TraceFlags,
		ServiceName:       d.ServiceName,
		Host:              d.Host,
		Pod:               d.Pod,
		Container:         d.Container,
		Environment:       d.Environment,
		AttributesString:  d.AttributesString,
		AttributesNumber:  d.AttributesNumber,
		AttributesBool:    d.AttributesBool,
		ScopeName:         d.ScopeName,
		ScopeVersion:      d.ScopeVersion,
	}
}

// MapLogs is the slice form of MapLog.
func MapLogs(rows []LogRow) []Log {
	out := make([]Log, len(rows))
	for i, r := range rows {
		out[i] = MapLog(r)
	}
	return out
}
