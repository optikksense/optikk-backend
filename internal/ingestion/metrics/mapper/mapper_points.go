package mapper

import (
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// appendGaugeRows emits one Row per Gauge data point.
func appendGaugeRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, dps []*metricsdatapb.NumberDataPoint) []*schema.Row {
	for _, dp := range dps {
		rows = append(rows, &schema.Row{
			TeamId:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.GetName(),
			MetricType:          "Gauge",
			Temporality:         "Unspecified",
			IsMonotonic:         false,
			Unit:                m.GetUnit(),
			Description:         m.GetDescription(),
			ResourceFingerprint: hdr.fingerprint,
			TimestampNs:         int64(dp.GetTimeUnixNano()), //nolint:gosec
			Value:               numberDataPointValue(dp),
			Attributes:          otlp.MergeAttrsMap(hdr.resMap, dp.GetAttributes()),
		})
	}
	return rows
}

// appendSumRows emits one Row per Sum data point.
func appendSumRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, sum *metricsdatapb.Sum) []*schema.Row {
	temporality := temporalityString(sum.GetAggregationTemporality())
	for _, dp := range sum.GetDataPoints() {
		rows = append(rows, &schema.Row{
			TeamId:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.GetName(),
			MetricType:          "Sum",
			Temporality:         temporality,
			IsMonotonic:         sum.GetIsMonotonic(),
			Unit:                m.GetUnit(),
			Description:         m.GetDescription(),
			ResourceFingerprint: hdr.fingerprint,
			TimestampNs:         int64(dp.GetTimeUnixNano()), //nolint:gosec
			Value:               numberDataPointValue(dp),
			Attributes:          otlp.MergeAttrsMap(hdr.resMap, dp.GetAttributes()),
		})
	}
	return rows
}

// appendHistogramRows emits one Row per Histogram data point. Value carries the
// bucket-weighted average for quick-glance charts; hist_sum/count/buckets carry
// the full distribution for percentile queries.
func appendHistogramRows(rows []*schema.Row, hdr header, m *metricsdatapb.Metric, hist *metricsdatapb.Histogram) []*schema.Row {
	temporality := temporalityString(hist.GetAggregationTemporality())
	for _, dp := range hist.GetDataPoints() {
		sum, avg := histValues(dp)
		rows = append(rows, &schema.Row{
			TeamId:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.GetName(),
			MetricType:          "Histogram",
			Temporality:         temporality,
			IsMonotonic:         false,
			Unit:                m.GetUnit(),
			Description:         m.GetDescription(),
			ResourceFingerprint: hdr.fingerprint,
			TimestampNs:         int64(dp.GetTimeUnixNano()), //nolint:gosec
			Value:               avg,
			HistSum:             sum,
			HistCount:           dp.GetCount(),
			HistBuckets:         dp.GetExplicitBounds(),
			HistCounts:          dp.GetBucketCounts(),
			Attributes:          otlp.MergeAttrsMap(hdr.resMap, dp.GetAttributes()),
		})
	}
	return rows
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
