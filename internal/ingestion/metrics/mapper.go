package metrics

import (
	"math"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/infra/fingerprint"
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	teamID      uint32
	fingerprint string
	resMap      map[string]string
}

func mapRequest(teamID int64, req *metricspb.ExportMetricsServiceRequest) []*schema.Row {
	var rows []*schema.Row
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := rowHeader{
			teamID:      uint32(teamID), //nolint:gosec
			fingerprint: fingerprint.Calculate(resMap),
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

func appendMetric(rows []*schema.Row, hdr rowHeader, m *metricsdatapb.Metric) []*schema.Row {
	switch data := m.Data.(type) {
	case *metricsdatapb.Metric_Gauge:
		for _, dp := range data.Gauge.GetDataPoints() {
			rows = append(rows, gaugeRow(hdr, m, dp))
		}
	case *metricsdatapb.Metric_Sum:
		temp := temporalityString(data.Sum.GetAggregationTemporality())
		for _, dp := range data.Sum.GetDataPoints() {
			rows = append(rows, sumRow(hdr, m, temp, data.Sum.GetIsMonotonic(), dp))
		}
	case *metricsdatapb.Metric_Histogram:
		temp := temporalityString(data.Histogram.GetAggregationTemporality())
		for _, dp := range data.Histogram.GetDataPoints() {
			rows = append(rows, histogramRow(hdr, m, temp, dp))
		}
	}
	return rows
}

func gaugeRow(hdr rowHeader, m *metricsdatapb.Metric, dp *metricsdatapb.NumberDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, "Gauge", "Unspecified", false, tsNs, attrs, numberValue(dp))
}

func sumRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, isMono bool, dp *metricsdatapb.NumberDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, "Sum", temporality, isMono, tsNs, attrs, numberValue(dp))
}

// histogramRow emits one Row per OTel histogram data point. The OTel
// `explicit_bounds` array carries N-1 boundaries for N buckets; the implicit
// last bucket has upper bound +Inf. We carry the explicit bounds plus +Inf in
// hist_buckets, and the per-bucket counts unchanged in hist_counts.
func histogramRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, dp *metricsdatapb.HistogramDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	sum := 0.0
	if dp.Sum != nil {
		sum = *dp.Sum
	}
	bounds := dp.GetExplicitBounds()
	counts := dp.GetBucketCounts()
	histBuckets := make([]float64, len(counts))
	for i := range counts {
		if i < len(bounds) {
			histBuckets[i] = bounds[i]
		} else {
			histBuckets[i] = math.Inf(1)
		}
	}
	row := baseRow(hdr, m, "Histogram", temporality, false, tsNs, attrs, 0)
	row.HistSum = sum
	row.HistCount = dp.GetCount()
	row.HistBuckets = histBuckets
	row.HistCounts = append([]uint64(nil), counts...)
	return row
}

func scalarRow(hdr rowHeader, m *metricsdatapb.Metric, metricType, temporality string, isMonotonic bool, tsNs int64, attrs map[string]string, value float64) *schema.Row {
	return baseRow(hdr, m, metricType, temporality, isMonotonic, tsNs, attrs, value)
}

func baseRow(
	hdr rowHeader, m *metricsdatapb.Metric,
	metricType, temporality string, isMonotonic bool,
	tsNs int64, attrs map[string]string,
	value float64,
) *schema.Row {
	bucket := timebucket.BucketStart(tsNs / 1_000_000_000)
	return &schema.Row{
		TeamId:              hdr.teamID,
		MetricName:          m.GetName(),
		MetricType:          metricType,
		Temporality:         temporality,
		IsMonotonic:         isMonotonic,
		Unit:                m.GetUnit(),
		Description:         m.GetDescription(),
		Fingerprint:         hdr.fingerprint,
		TimestampNs:         tsNs,
		TsBucketHourSeconds: int64(bucket),
		Value:               value,
		Resource:            hdr.resMap,
		Attributes:          attrs,
		Service:             hdr.resMap["service.name"],
		Host:                hdr.resMap["host.name"],
		Environment:         hdr.resMap["deployment.environment"],
		K8SNamespace:        hdr.resMap["k8s.namespace.name"],
		HttpMethod:          attrs["http.method"],
		HttpStatusCode:      parseHTTPStatus(attrs["http.status_code"]),
	}
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

func numberValue(dp *metricsdatapb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsdatapb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsdatapb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}

func parseHTTPStatus(s string) uint32 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(v)
}
