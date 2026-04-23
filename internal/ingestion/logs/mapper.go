package logs

import (
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/enrich"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const nsPerSecond = 1_000_000_000

// MapRequest converts an OTLP log export request into wire rows ready to be
// produced to Kafka. The caller owns teamID resolution — it must come from
// auth.TeamIDFromContext on the RPC boundary.
func MapRequest(teamID int64, req *logspb.ExportLogsServiceRequest) []*Row {
	var rows []*Row
	for _, rl := range req.GetResourceLogs() {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := otlp.AttrsToMap(resAttrs)
		fingerprint := strconv.FormatUint(otlp.ResourceFingerprint(resAttrs), 16)
		for _, sl := range rl.GetScopeLogs() {
			scopeName, scopeVersion := extractScope(sl.GetScope())
			for _, lr := range sl.GetLogRecords() {
				rows = append(rows, buildRow(teamID, resourceMap, fingerprint, scopeName, scopeVersion, lr))
			}
		}
	}
	return rows
}

// buildRow maps a single OTLP log record into a wire Row. Attribute capping is
// kept inside mapper_attrs.go so this function stays under the 40-LOC cap.
// The result is routed through enrich.Enrich so downstream consumers see the
// same normalized shape whether the row came from OTLP or a reprocessing tool.
func buildRow(teamID int64, resource map[string]string, fingerprint, scopeName, scopeVersion string, lr *logv1.LogRecord) *Row {
	tsNs, observedNs := resolveTimestamps(lr)
	tsBucket := utils.LogsBucketStart(int64(tsNs / nsPerSecond))
	attrStr, attrNum, attrBool := typedAttrs(lr.GetAttributes())
	attrStr = capStringAttrs(attrStr, teamID)
	sevNum := uint32(lr.GetSeverityNumber()) //nolint:gosec // OTLP 0..24
	return &Row{
		TeamId:              uint32(teamID), //nolint:gosec // G115 team_id
		TsBucketStart:       tsBucket,
		TimestampNs:         int64(tsNs), //nolint:gosec // ns fits int64
		ObservedTimestampNs: observedNs,
		TraceId:             enrich.ZeroTraceID(otlp.BytesToHex(lr.GetTraceId())),
		SpanId:              enrich.ZeroSpanID(otlp.BytesToHex(lr.GetSpanId())),
		TraceFlags:          lr.GetFlags(),
		SeverityText:        enrich.NormalizeSeverityText(resolveSeverity(lr), sevNum),
		SeverityNumber:      sevNum,
		Body:                otlp.AnyValueString(lr.GetBody()),
		AttributesString:    attrStr,
		AttributesNumber:    attrNum,
		AttributesBool:      attrBool,
		Resource:            enrich.FillResourceFallbacks(resource, attrStr),
		ResourceFingerprint: fingerprint,
		ScopeName:           scopeName,
		ScopeVersion:        scopeVersion,
	}
}

// resolveTimestamps picks the best available timestamp, falling back to now().
func resolveTimestamps(lr *logv1.LogRecord) (tsNs, observedNs uint64) {
	tsNs = lr.GetTimeUnixNano()
	if tsNs == 0 {
		tsNs = lr.GetObservedTimeUnixNano()
	}
	if tsNs == 0 {
		tsNs = uint64(time.Now().UnixNano()) //nolint:gosec // G115
	}
	observedNs = lr.GetObservedTimeUnixNano()
	if observedNs == 0 {
		observedNs = uint64(time.Now().UnixNano()) //nolint:gosec // G115
	}
	return tsNs, observedNs
}

// resolveSeverity returns severity text, falling back to the numeric level name.
func resolveSeverity(lr *logv1.LogRecord) string {
	if s := lr.GetSeverityText(); s != "" {
		return s
	}
	return severityNumberToLevel(lr.GetSeverityNumber())
}

// extractScope flattens an instrumentation scope into (name, version). The
// old (name, version, attrs) form is retired alongside the `scope_string`
// column drop — the attrs map was a duplicate of name and unused downstream.
func extractScope(scope *commonpb.InstrumentationScope) (string, string) {
	if scope == nil {
		return "", ""
	}
	return scope.GetName(), scope.GetVersion()
}

func severityNumberToLevel(n logv1.SeverityNumber) string {
	v := int(n)
	switch {
	case v <= 0:
		return "UNSET"
	case v <= 4:
		return "TRACE"
	case v <= 8:
		return "DEBUG"
	case v <= 12:
		return "INFO"
	case v <= 16:
		return "WARN"
	case v <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}
