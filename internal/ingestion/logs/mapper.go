package logs

import (
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/fingerprint"
	obsmetrics "github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const (
	nsPerSecond      = 1_000_000_000
	maxLogAttributes = 128
	zeroTraceHex     = "00000000000000000000000000000000"
	zeroSpanHex      = "0000000000000000"
)

func mapRequest(teamID int64, req *logspb.ExportLogsServiceRequest) []*schema.Row {
	nowNs := uint64(time.Now().UnixNano()) //nolint:gosec
	var rows []*schema.Row
	for _, rl := range req.GetResourceLogs() {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := otlp.AttrsToMap(resAttrs)
		fp := fingerprint.Calculate(resourceMap)
		for _, sl := range rl.GetScopeLogs() {
			scopeName, scopeVersion := "", ""
			if sl.GetScope() != nil {
				scopeName = sl.GetScope().GetName()
				scopeVersion = sl.GetScope().GetVersion()
			}
			for _, lr := range sl.GetLogRecords() {
				rows = append(rows, buildLogRow(teamID, resourceMap, fp, scopeName, scopeVersion, lr, nowNs))
			}
		}
	}
	return rows
}

func buildLogRow(teamID int64, resource map[string]string, fp, scopeName, scopeVersion string, lr *logv1.LogRecord, nowNs uint64) *schema.Row {
	tsNs := lr.GetTimeUnixNano()
	if tsNs == 0 {
		tsNs = lr.GetObservedTimeUnixNano()
	}
	if tsNs == 0 {
		tsNs = nowNs
	}
	observedNs := lr.GetObservedTimeUnixNano()
	if observedNs == 0 {
		observedNs = nowNs
	}
	tsBucket := timebucket.LogsBucketStart(int64(tsNs / nsPerSecond))

	attrStr, attrNum, attrBool, dropped := otlp.TypedAttrs(lr.GetAttributes(), maxLogAttributes)
	if dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("logs").Add(float64(dropped))
	}
	sevNum := uint32(lr.GetSeverityNumber()) //nolint:gosec
	res := fillResourceFallbacks(resource, attrStr)

	return &schema.Row{
		TeamId:              uint32(teamID), //nolint:gosec
		TsBucket:            tsBucket,
		TimestampNs:         int64(tsNs), //nolint:gosec
		ObservedTimestampNs: observedNs,
		TraceId:             zeroOut(otlp.BytesToHex(lr.GetTraceId()), zeroTraceHex),
		SpanId:              zeroOut(otlp.BytesToHex(lr.GetSpanId()), zeroSpanHex),
		TraceFlags:          lr.GetFlags(),
		SeverityText:        normalizeSeverityText(resolveSeverity(lr), sevNum),
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

func resolveSeverity(lr *logv1.LogRecord) string {
	if s := lr.GetSeverityText(); s != "" {
		return s
	}
	return severityNumberToLevel(lr.GetSeverityNumber())
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

func normalizeSeverityText(text string, num uint32) string {
	t := strings.ToUpper(strings.TrimSpace(text))
	if t != "" {
		return t
	}
	switch {
	case num == 0:
		return "UNSET"
	case num <= 4:
		return "TRACE"
	case num <= 8:
		return "DEBUG"
	case num <= 12:
		return "INFO"
	case num <= 16:
		return "WARN"
	case num <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

func fillResourceFallbacks(resource, attrs map[string]string) map[string]string {
	if resource == nil {
		resource = map[string]string{}
	}
	for _, k := range []string{"service.name", "host.name", "k8s.pod.name", "deployment.environment"} {
		if resource[k] != "" {
			continue
		}
		if v := attrs[k]; v != "" {
			resource[k] = v
		}
	}
	return resource
}

func zeroOut(id, zero string) string {
	if id == zero {
		return ""
	}
	return id
}
