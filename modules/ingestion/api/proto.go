package api

import (
	"io"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// isProtobuf returns true if the request carries a protobuf content-type.
func isProtobuf(c *gin.Context) bool {
	ct := c.GetHeader("Content-Type")
	return strings.Contains(ct, "application/x-protobuf") || strings.Contains(ct, "application/protobuf")
}

// readBody reads up to 100 MB from the request body.
func readBody(c *gin.Context) ([]byte, error) {
	return io.ReadAll(io.LimitReader(c.Request.Body, 100<<20))
}

// ---------- Traces proto → internal conversion ----------

func protoToTracesPayload(body []byte) (model.OTLPTracesPayload, error) {
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

// ---------- Metrics proto → internal conversion ----------

func protoToMetricsPayload(body []byte) (model.OTLPMetricsPayload, error) {
	var req colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return model.OTLPMetricsPayload{}, err
	}
	var out model.OTLPMetricsPayload
	for _, rm := range req.GetResourceMetrics() {
		orm := model.OTLPResourceMetrics{
			Resource: protoResource(rm.GetResource()),
		}
		for _, sm := range rm.GetScopeMetrics() {
			osm := model.OTLPScopeMetrics{
				Scope: protoScope(sm.GetScope()),
			}
			for _, m := range sm.GetMetrics() {
				om := model.OTLPMetric{
					Name:        m.GetName(),
					Description: m.GetDescription(),
					Unit:        m.GetUnit(),
				}
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					om.Gauge = &model.OTLPGauge{}
					for _, dp := range d.Gauge.GetDataPoints() {
						om.Gauge.DataPoints = append(om.Gauge.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Sum:
					om.Sum = &model.OTLPSum{
						AggregationTemporality: int(d.Sum.GetAggregationTemporality()),
						IsMonotonic:            d.Sum.GetIsMonotonic(),
					}
					for _, dp := range d.Sum.GetDataPoints() {
						om.Sum.DataPoints = append(om.Sum.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Histogram:
					om.Histogram = &model.OTLPHistogram{
						AggregationTemporality: int(d.Histogram.GetAggregationTemporality()),
					}
					for _, dp := range d.Histogram.GetDataPoints() {
						om.Histogram.DataPoints = append(om.Histogram.DataPoints, protoHistogramDP(dp))
					}
				}
				osm.Metrics = append(osm.Metrics, om)
			}
			orm.ScopeMetrics = append(orm.ScopeMetrics, osm)
		}
		out.ResourceMetrics = append(out.ResourceMetrics, orm)
	}
	return out, nil
}

// ---------- Logs proto → internal conversion ----------

func protoToLogsPayload(body []byte) (model.OTLPLogsPayload, error) {
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

// ---------- Shared proto helpers ----------

func protoResource(r *resourcepb.Resource) model.OTLPResource {
	if r == nil {
		return model.OTLPResource{}
	}
	return model.OTLPResource{Attributes: protoAttrs(r.GetAttributes())}
}

func protoScope(s *commonpb.InstrumentationScope) model.OTLPScope {
	if s == nil {
		return model.OTLPScope{}
	}
	return model.OTLPScope{Name: s.GetName(), Version: s.GetVersion()}
}

func protoAttrs(attrs []*commonpb.KeyValue) []model.OTLPAttribute {
	out := make([]model.OTLPAttribute, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, model.OTLPAttribute{
			Key:   kv.GetKey(),
			Value: protoAnyValue(kv.GetValue()),
		})
	}
	return out
}

func protoAnyValue(v *commonpb.AnyValue) model.OTLPAnyValue {
	if v == nil {
		return model.OTLPAnyValue{}
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		s := val.StringValue
		return model.OTLPAnyValue{StringValue: &s}
	case *commonpb.AnyValue_IntValue:
		s := intStr(val.IntValue)
		return model.OTLPAnyValue{IntValue: &s}
	case *commonpb.AnyValue_DoubleValue:
		f := val.DoubleValue
		return model.OTLPAnyValue{DoubleValue: &f}
	case *commonpb.AnyValue_BoolValue:
		b := val.BoolValue
		return model.OTLPAnyValue{BoolValue: &b}
	default:
		return model.OTLPAnyValue{}
	}
}

func protoNumberDP(dp *metricspb.NumberDataPoint) model.OTLPNumberDataPoint {
	out := model.OTLPNumberDataPoint{
		Attributes:        protoAttrs(dp.GetAttributes()),
		StartTimeUnixNano: uint64Str(dp.GetStartTimeUnixNano()),
		TimeUnixNano:      uint64Str(dp.GetTimeUnixNano()),
	}
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		out.AsDouble = &v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		s := intStr(v.AsInt)
		out.AsInt = &s
	}
	return out
}

func protoHistogramDP(dp *metricspb.HistogramDataPoint) model.OTLPHistogramDataPoint {
	out := model.OTLPHistogramDataPoint{
		Attributes:        protoAttrs(dp.GetAttributes()),
		StartTimeUnixNano: uint64Str(dp.GetStartTimeUnixNano()),
		TimeUnixNano:      uint64Str(dp.GetTimeUnixNano()),
		Count:             uint64Str(dp.GetCount()),
		ExplicitBounds:    dp.GetExplicitBounds(),
	}
	if dp.Sum != nil {
		s := dp.GetSum()
		out.Sum = &s
	}
	if dp.Min != nil {
		m := dp.GetMin()
		out.Min = &m
	}
	if dp.Max != nil {
		m := dp.GetMax()
		out.Max = &m
	}
	for _, bc := range dp.GetBucketCounts() {
		out.BucketCounts = append(out.BucketCounts, uint64Str(bc))
	}
	return out
}

func hexBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	const hex = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hex[v>>4]
		out[i*2+1] = hex[v&0x0f]
	}
	return string(out)
}

func uint64Str(v uint64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatUint(v, 10)
}

func intStr(v int64) string {
	return strconv.FormatInt(v, 10)
}
