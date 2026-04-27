package models

// RawLogsTable is the raw observability.logs table every read submodule
// PREWHEREs into.
const RawLogsTable = "observability.logs"

// LogColumns is the canonical SELECT projection that hydrates a LogRow.
const LogColumns = `timestamp, observed_timestamp, severity_text, severity_number, severity_bucket,
	body, trace_id, span_id, trace_flags,
	service, host, pod, container, environment,
	attributes_string, attributes_number, attributes_bool,
	scope_name, scope_version`

// PickLimit clamps an int request param to [1, max] with a default fallback.
func PickLimit(v, def, maxLimit int) int {
	if v <= 0 {
		return def
	}
	if v > maxLimit {
		return maxLimit
	}
	return v
}
