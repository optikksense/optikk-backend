package telemetry

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/proto"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// ProtoDecoder converts raw bytes into a typed OTLP payload.
type ProtoDecoder[T any] func([]byte) (T, error)

// DecodePayload reads and decodes the request body as protobuf or JSON.
func DecodePayload[T any](c *gin.Context, protoDec ProtoDecoder[T]) (T, error) {
	var zero T
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 100<<20))
	if err != nil {
		return zero, fmt.Errorf("failed to read request body")
	}
	ct := c.GetHeader("Content-Type")
	if strings.Contains(ct, "application/x-protobuf") || strings.Contains(ct, "application/protobuf") {
		return protoDec(body)
	}
	var payload T
	if err := json.Unmarshal(body, &payload); err != nil {
		return zero, fmt.Errorf("invalid OTLP payload")
	}
	return payload, nil
}

// ---------------------------------------------------------------------------
// Protobuf → OTLP model converters
// ---------------------------------------------------------------------------

func ProtoToTracesPayload(body []byte) (OTLPTracesPayload, error) {
	var req coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return OTLPTracesPayload{}, err
	}
	var out OTLPTracesPayload
	for _, rs := range req.GetResourceSpans() {
		ors := OTLPResourceSpans{Resource: protoResource(rs.GetResource())}
		for _, ss := range rs.GetScopeSpans() {
			oss := OTLPScopeSpans{Scope: protoScope(ss.GetScope())}
			for _, span := range ss.GetSpans() {
				s := OTLPSpan{
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
					s.Status = &OTLPStatus{Code: int(st.GetCode()), Message: st.GetMessage()}
				}
				oss.Spans = append(oss.Spans, s)
			}
			ors.ScopeSpans = append(ors.ScopeSpans, oss)
		}
		out.ResourceSpans = append(out.ResourceSpans, ors)
	}
	return out, nil
}

func ProtoToMetricsPayload(body []byte) (OTLPMetricsPayload, error) {
	var req colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return OTLPMetricsPayload{}, err
	}
	var out OTLPMetricsPayload
	for _, rm := range req.GetResourceMetrics() {
		orm := OTLPResourceMetrics{Resource: protoResource(rm.GetResource())}
		for _, sm := range rm.GetScopeMetrics() {
			osm := OTLPScopeMetrics{Scope: protoScope(sm.GetScope())}
			for _, m := range sm.GetMetrics() {
				om := OTLPMetric{Name: m.GetName(), Description: m.GetDescription(), Unit: m.GetUnit()}
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					om.Gauge = &OTLPGauge{}
					for _, dp := range d.Gauge.GetDataPoints() {
						om.Gauge.DataPoints = append(om.Gauge.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Sum:
					om.Sum = &OTLPSum{AggregationTemporality: int(d.Sum.GetAggregationTemporality()), IsMonotonic: d.Sum.GetIsMonotonic()}
					for _, dp := range d.Sum.GetDataPoints() {
						om.Sum.DataPoints = append(om.Sum.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Histogram:
					om.Histogram = &OTLPHistogram{AggregationTemporality: int(d.Histogram.GetAggregationTemporality())}
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

func ProtoToLogsPayload(body []byte) (OTLPLogsPayload, error) {
	var req collogspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return OTLPLogsPayload{}, err
	}
	var out OTLPLogsPayload
	for _, rl := range req.GetResourceLogs() {
		orl := OTLPResourceLogs{Resource: protoResource(rl.GetResource())}
		for _, sl := range rl.GetScopeLogs() {
			osl := OTLPScopeLogs{Scope: protoScope(sl.GetScope())}
			for _, lr := range sl.GetLogRecords() {
				osl.LogRecords = append(osl.LogRecords, OTLPLogRecord{
					TimeUnixNano:         uint64Str(lr.GetTimeUnixNano()),
					ObservedTimeUnixNano: uint64Str(lr.GetObservedTimeUnixNano()),
					SeverityNumber:       int(lr.GetSeverityNumber()),
					SeverityText:         lr.GetSeverityText(),
					Body:                 protoAnyValue(lr.GetBody()),
					Attributes:           protoAttrs(lr.GetAttributes()),
					TraceID:              hexBytes(lr.GetTraceId()),
					SpanID:               hexBytes(lr.GetSpanId()),
				})
			}
			orl.ScopeLogs = append(orl.ScopeLogs, osl)
		}
		out.ResourceLogs = append(out.ResourceLogs, orl)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Proto helpers
// ---------------------------------------------------------------------------

func protoResource(r *resourcepb.Resource) OTLPResource {
	if r == nil {
		return OTLPResource{}
	}
	return OTLPResource{Attributes: protoAttrs(r.GetAttributes())}
}

func protoScope(s *commonpb.InstrumentationScope) OTLPScope {
	if s == nil {
		return OTLPScope{}
	}
	return OTLPScope{Name: s.GetName(), Version: s.GetVersion()}
}

func protoAttrs(attrs []*commonpb.KeyValue) []OTLPAttribute {
	out := make([]OTLPAttribute, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, OTLPAttribute{Key: kv.GetKey(), Value: protoAnyValue(kv.GetValue())})
	}
	return out
}

func protoAnyValue(v *commonpb.AnyValue) OTLPAnyValue {
	if v == nil {
		return OTLPAnyValue{}
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		s := val.StringValue
		return OTLPAnyValue{StringValue: &s}
	case *commonpb.AnyValue_IntValue:
		s := intStr(val.IntValue)
		return OTLPAnyValue{IntValue: &s}
	case *commonpb.AnyValue_DoubleValue:
		f := val.DoubleValue
		return OTLPAnyValue{DoubleValue: &f}
	case *commonpb.AnyValue_BoolValue:
		b := val.BoolValue
		return OTLPAnyValue{BoolValue: &b}
	default:
		return OTLPAnyValue{}
	}
}

func protoNumberDP(dp *metricspb.NumberDataPoint) OTLPNumberDataPoint {
	out := OTLPNumberDataPoint{
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

func protoHistogramDP(dp *metricspb.HistogramDataPoint) OTLPHistogramDataPoint {
	out := OTLPHistogramDataPoint{
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
	const hx = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hx[v>>4]
		out[i*2+1] = hx[v&0x0f]
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
