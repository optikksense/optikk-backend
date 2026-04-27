package mapper

import (
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func appendGaugeRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, dps []*metricsdatapb.NumberDataPoint) []*schema.Row {
	for _, dp := range dps {
		tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
		attrs := otlp.AttrsToMap(dp.GetAttributes())
		rows = append(rows, baseRow(hdr, m, "Gauge", "Unspecified", false, tsNs, hourBucketSeconds(tsNs), attrs, numberDataPointValue(dp), 0, 0, nil, nil))
	}
	return rows
}

func appendSumRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, sum *metricsdatapb.Sum) []*schema.Row {
	temporality := temporalityString(sum.GetAggregationTemporality())
	for _, dp := range sum.GetDataPoints() {
		tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
		attrs := otlp.AttrsToMap(dp.GetAttributes())
		rows = append(rows, baseRow(hdr, m, "Sum", temporality, sum.GetIsMonotonic(), tsNs, hourBucketSeconds(tsNs), attrs, numberDataPointValue(dp), 0, 0, nil, nil))
	}
	return rows
}

// appendHistogramRows emits one Row per Histogram data point with bucket-weighted average in Value plus full distribution columns.
func appendHistogramRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, hist *metricsdatapb.Histogram) []*schema.Row {
	temporality := temporalityString(hist.GetAggregationTemporality())
	for _, dp := range hist.GetDataPoints() {
		sum, avg := histValues(dp)
		tsNs := int64(dp.GetTimeUnixNano()) //nolint:gosec
		attrs := otlp.AttrsToMap(dp.GetAttributes())
		rows = append(rows, baseRow(hdr, m, "Histogram", temporality, false, tsNs, hourBucketSeconds(tsNs), attrs, avg, sum, dp.GetCount(), dp.GetExplicitBounds(), dp.GetBucketCounts()))
	}
	return rows
}

func baseRow(
	hdr header, m *metricsdatapb.Metric,
	metricType, temporality string, isMonotonic bool,
	tsNs, hourSec int64,
	attrs map[string]string,
	value, histSum float64, histCount uint64, histBuckets []float64, histCounts []uint64,
) *schema.Row {
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
		TsBucketHourSeconds: hourSec,
		Value:               value,
		HistSum:             histSum,
		HistCount:           histCount,
		HistBuckets:         histBuckets,
		HistCounts:          histCounts,
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

// histValues extracts sum and bucket-weighted average from a histogram point.
func histValues(dp *metricsdatapb.HistogramDataPoint) (sum, avg float64) {
	if dp.Sum != nil {
		sum = *dp.Sum
	}
	if dp.Count > 0 {
		avg = sum / float64(dp.Count)
	}
	return sum, avg
}
