package models

// EncodeLogID returns the row's deep-link id — the stable FNV-64a hex hash of
// (trace_id, timestamp_ns, body, fingerprint) computed by the ingestion mapper
// (see internal/ingestion/logs/mapper.go::computeLogID) and stored as the
// `log_id` column on observability.logs.
func EncodeLogID(d LogRow) string {
	return d.LogID
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
