package decode

import (
	"strconv"

	"github.com/observability/observability-backend-go/modules/ingestion/model"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

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
