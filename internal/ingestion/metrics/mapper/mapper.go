// Package mapper converts OTLP metric export requests into metrics/schema.Row
// wire values ready to be produced to Kafka. Each data point in an OTLP
// request yields one Row.
package mapper

import (
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// header is the subset of resource-scoped context that every emitted Row
// needs. Passed through the mapper_points helpers so each point builder stays
// under the function-size cap.
type header struct {
	teamID      uint32
	env         string
	fingerprint uint64
	resMap      map[string]string
}

// MapRequest converts an OTLP metrics export request into wire rows ready to be
// produced to Kafka. One data point == one Row; histogram data points emit
// one Row each.
func MapRequest(teamID int64, req *metricspb.ExportMetricsServiceRequest) []*schema.Row {
	var rows []*schema.Row
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := header{
			teamID:      uint32(teamID), //nolint:gosec // team id
			env:         envFromResource(resMap),
			fingerprint: otlp.ResourceFingerprint(resAttrs),
			resMap:      resMap,
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				rows = appendMetric(rows, hdr, m)
			}
		}
	}
	return rows
}

// appendMetric dispatches on the metric data type so the per-type helpers in
// mapper_points.go stay small. Summary / ExponentialHistogram are ignored for
// now (they were ignored in the legacy code too).
func appendMetric(rows []*schema.Row, hdr header, m *metricsdatapb.Metric) []*schema.Row {
	switch data := m.Data.(type) {
	case *metricsdatapb.Metric_Gauge:
		return appendGaugeRows(rows, hdr, m, data.Gauge.GetDataPoints())
	case *metricsdatapb.Metric_Sum:
		return appendSumRows(rows, hdr, m, data.Sum)
	case *metricsdatapb.Metric_Histogram:
		return appendHistogramRows(rows, hdr, m, data.Histogram)
	}
	return rows
}

func envFromResource(resMap map[string]string) string {
	if env := resMap["deployment.environment"]; env != "" {
		return env
	}
	return ""
}

func temporalityString(t metricsdatapb.AggregationTemporality) string {
	switch t {
	case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return "Delta"
	case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return "Cumulative"
	default:
		return "Unspecified"
	}
}

func numberDataPointValue(dp *metricsdatapb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsdatapb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsdatapb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}
