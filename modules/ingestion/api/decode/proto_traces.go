package decode

import (
	"github.com/observability/observability-backend-go/modules/ingestion/model"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// ProtoToTracesPayload unmarshals a protobuf ExportTraceServiceRequest into the internal model.
func ProtoToTracesPayload(body []byte) (model.OTLPTracesPayload, error) {
	var req coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return model.OTLPTracesPayload{}, err
	}
	var out model.OTLPTracesPayload
	for _, rs := range req.GetResourceSpans() {
		ors := model.OTLPResourceSpans{
			Resource: protoResource(rs.GetResource()),
		}
		for _, ss := range rs.GetScopeSpans() {
			oss := model.OTLPScopeSpans{
				Scope: protoScope(ss.GetScope()),
			}
			for _, span := range ss.GetSpans() {
				s := model.OTLPSpan{
					TraceID:           hexBytes(span.GetTraceId()),
					SpanID:            hexBytes(span.GetSpanId()),
					ParentSpanID:      hexBytes(span.GetParentSpanId()),
					Name:              span.GetName(),
					Kind:              int(span.GetKind()),
					StartTimeUnixNano: uint64Str(span.GetStartTimeUnixNano()),
					EndTimeUnixNano:   uint64Str(span.GetEndTimeUnixNano()),
					Attributes:        protoAttrs(span.GetAttributes()),
				}
				if st := span.GetStatus(); st != nil {
					s.Status = &model.OTLPStatus{
						Code:    int(st.GetCode()),
						Message: st.GetMessage(),
					}
				}
				oss.Spans = append(oss.Spans, s)
			}
			ors.ScopeSpans = append(ors.ScopeSpans, oss)
		}
		out.ResourceSpans = append(out.ResourceSpans, ors)
	}
	return out, nil
}
