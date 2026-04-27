// Package mapper converts OTLP log export requests into logs/schema.Row wire values.
package mapper

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/fingerprint"
	obsmetrics "github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/enrich"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const nsPerSecond = 1_000_000_000

// maxLogAttributes bounds the attributes_string map per row. Over-limit keys
// are dropped in deterministic sort order via otlp.TypedAttrs; the dropped
// count feeds the mapper_attrs_dropped_total counter.
const maxLogAttributes = 128

// MapRequest converts an OTLP log export request into wire rows; one OTLP record yields one Row.
func MapRequest(teamID int64, req *logspb.ExportLogsServiceRequest) []*schema.Row {
	nowNs := uint64(time.Now().UnixNano()) //nolint:gosec // nanotime fits uint64
	var rows []*schema.Row
	for _, rl := range req.GetResourceLogs() {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := otlp.AttrsToMap(resAttrs)
		fp := fingerprint.Calculate(resourceMap)
		for _, sl := range rl.GetScopeLogs() {
			scopeName, scopeVersion := extractScope(sl.GetScope())
			for _, lr := range sl.GetLogRecords() {
				rows = append(rows, buildRow(teamID, resourceMap, fp, scopeName, scopeVersion, lr, nowNs))
			}
		}
	}
	return rows
}

func buildRow(teamID int64, resource map[string]string, fp, scopeName, scopeVersion string, lr *logv1.LogRecord, nowNs uint64) *schema.Row {
	tsNs, observedNs := resolveTimestamps(lr, nowNs)
	tsBucket := timebucket.LogsBucketStart(int64(tsNs / nsPerSecond))
	attrStr, attrNum, attrBool, dropped := otlp.TypedAttrs(lr.GetAttributes(), maxLogAttributes)
	if dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("logs").Add(float64(dropped))
	}
	sevNum := uint32(lr.GetSeverityNumber()) //nolint:gosec // OTLP 0..24
	res := enrich.FillResourceFallbacks(resource, attrStr)
	return &schema.Row{
		TeamId:              uint32(teamID), //nolint:gosec // G115 team_id
		TsBucket:            tsBucket,
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
		Resource:            res,
		Fingerprint:         fp,
		ScopeName:           scopeName,
		ScopeVersion:        scopeVersion,
		Service:             res["service.name"],
		Host:                res["host.name"],
		Pod:                 res["k8s.pod.name"],
		Container:           res["k8s.container.name"],
		Environment:         res["deployment.environment"],
	}
}

func resolveTimestamps(lr *logv1.LogRecord, nowNs uint64) (tsNs, observedNs uint64) {
	tsNs = lr.GetTimeUnixNano()
	if tsNs == 0 {
		tsNs = lr.GetObservedTimeUnixNano()
	}
	if tsNs == 0 {
		tsNs = nowNs
	}
	observedNs = lr.GetObservedTimeUnixNano()
	if observedNs == 0 {
		observedNs = nowNs
	}
	return tsNs, observedNs
}

func resolveSeverity(lr *logv1.LogRecord) string {
	if s := lr.GetSeverityText(); s != "" {
		return s
	}
	return severityNumberToLevel(lr.GetSeverityNumber())
}

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
