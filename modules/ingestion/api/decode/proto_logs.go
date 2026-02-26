package decode

import (
	"github.com/observability/observability-backend-go/modules/ingestion/model"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

// ProtoToLogsPayload unmarshals a protobuf ExportLogsServiceRequest into the internal model.
func ProtoToLogsPayload(body []byte) (model.OTLPLogsPayload, error) {
	var req collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return model.OTLPLogsPayload{}, err
	}
	var out model.OTLPLogsPayload
	for _, rl := range req.GetResourceLogs() {
		orl := model.OTLPResourceLogs{
			Resource: protoResource(rl.GetResource()),
		}
		for _, sl := range rl.GetScopeLogs() {
			osl := model.OTLPScopeLogs{
				Scope: protoScope(sl.GetScope()),
			}
			for _, lr := range sl.GetLogRecords() {
				olr := model.OTLPLogRecord{
					TimeUnixNano:         uint64Str(lr.GetTimeUnixNano()),
					ObservedTimeUnixNano: uint64Str(lr.GetObservedTimeUnixNano()),
					SeverityNumber:       int(lr.GetSeverityNumber()),
					SeverityText:         lr.GetSeverityText(),
					Body:                 protoAnyValue(lr.GetBody()),
					Attributes:           protoAttrs(lr.GetAttributes()),
					TraceID:              hexBytes(lr.GetTraceId()),
					SpanID:               hexBytes(lr.GetSpanId()),
				}
				osl.LogRecords = append(osl.LogRecords, olr)
			}
			orl.ScopeLogs = append(orl.ScopeLogs, osl)
		}
		out.ResourceLogs = append(out.ResourceLogs, orl)
	}
	return out, nil
}
