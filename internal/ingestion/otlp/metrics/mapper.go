package metrics

import (
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/protoconv"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

const defaultEnv = "default"

var (
	emptyBounds = []float64{}
	emptyCounts = []uint64{}
)

// MetricColumns is the ClickHouse insert column order for observability.metrics.
var MetricColumns = []string{
	"team_id", "env", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "resource_fingerprint", "timestamp", "value",
	"hist_sum", "hist_count", "hist_buckets", "hist_counts", "attributes",
}

type metricHeader struct {
	teamID      uint32
	env         string
	fingerprint uint64
	resMap      map[string]string
}

// mapMetrics converts an OTLP metrics export request into ClickHouse ingest rows.
func mapMetrics(teamID int64, req *metricspb.ExportMetricsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rm := range req.ResourceMetrics {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := protoconv.AttrsToMap(resAttrs)
		hdr := metricHeader{
			teamID:      uint32(teamID), //nolint:gosec // G115
			env:         envFromResource(resMap),
			fingerprint: protoconv.ResourceFingerprint(resAttrs),
			resMap:      resMap,
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				switch data := m.Data.(type) {
				case *metricsdatapb.Metric_Gauge:
					rows = mapGaugeRows(rows, hdr, m, data.Gauge.DataPoints)
				case *metricsdatapb.Metric_Sum:
					rows = mapSumRows(rows, hdr, m, data.Sum)
				case *metricsdatapb.Metric_Histogram:
					rows = mapHistogramRows(rows, hdr, m, data.Histogram)
				}
			}
		}
	}
	return rows
}

// envFromResource returns the deployment environment or "default".
func envFromResource(resMap map[string]string) string {
	if env := resMap["deployment.environment"]; env != "" {
		return env
	}
	return defaultEnv
}

// mapGaugeRows appends one row per Gauge data point.
func mapGaugeRows(rows []ingest.Row, hdr metricHeader, m *metricsdatapb.Metric, dps []*metricsdatapb.NumberDataPoint) []ingest.Row {
	for _, dp := range dps {
		rows = append(rows, ingest.Row{Values: []any{
			hdr.teamID,                            // team_id
			hdr.env,                               // env
			m.Name,                                // metric_name
			"Gauge",                               // metric_type
			"Unspecified",                         // temporality
			false,                                 // is_monotonic
			m.Unit,                                // unit
			m.Description,                         // description
			hdr.fingerprint,                       // resource_fingerprint
			protoconv.NanoToTime(dp.TimeUnixNano), // timestamp
			numberDataPointValue(dp),              // value
			0.0,                                   // hist_sum
			uint64(0),                             // hist_count
			emptyBounds,                           // hist_buckets
			emptyCounts,                           // hist_counts
			protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes), // attributes
		}})
	}
	return rows
}

// mapSumRows appends one row per Sum data point.
func mapSumRows(rows []ingest.Row, hdr metricHeader, m *metricsdatapb.Metric, sum *metricsdatapb.Sum) []ingest.Row {
	temporality := temporalityString(sum.AggregationTemporality)
	for _, dp := range sum.DataPoints {
		rows = append(rows, ingest.Row{Values: []any{
			hdr.teamID,                            // team_id
			hdr.env,                               // env
			m.Name,                                // metric_name
			"Sum",                                 // metric_type
			temporality,                           // temporality
			sum.IsMonotonic,                       // is_monotonic
			m.Unit,                                // unit
			m.Description,                         // description
			hdr.fingerprint,                       // resource_fingerprint
			protoconv.NanoToTime(dp.TimeUnixNano), // timestamp
			numberDataPointValue(dp),              // value
			0.0,                                   // hist_sum
			uint64(0),                             // hist_count
			emptyBounds,                           // hist_buckets
			emptyCounts,                           // hist_counts
			protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes), // attributes
		}})
	}
	return rows
}

// mapHistogramRows appends one row per Histogram data point.
func mapHistogramRows(rows []ingest.Row, hdr metricHeader, m *metricsdatapb.Metric, hist *metricsdatapb.Histogram) []ingest.Row {
	temporality := temporalityString(hist.AggregationTemporality)
	for _, dp := range hist.DataPoints {
		sum, avg := histValues(dp)
		rows = append(rows, ingest.Row{Values: []any{
			hdr.teamID,                              // team_id
			hdr.env,                                 // env
			m.Name,                                  // metric_name
			"Histogram",                             // metric_type
			temporality,                             // temporality
			false,                                   // is_monotonic
			m.Unit,                                  // unit
			m.Description,                           // description
			hdr.fingerprint,                         // resource_fingerprint
			protoconv.NanoToTime(dp.TimeUnixNano),   // timestamp
			avg,                                     // value
			sum,                                     // hist_sum
			dp.Count,                                // hist_count
			orSlice(dp.ExplicitBounds, emptyBounds), // hist_buckets
			orSlice(dp.BucketCounts, emptyCounts),   // hist_counts
			protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes), // attributes
		}})
	}
	return rows
}

// temporalityString converts an OTLP aggregation temporality enum to a string.
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

// histValues extracts sum and average from a histogram data point.
func histValues(dp *metricsdatapb.HistogramDataPoint) (sum, avg float64) {
	if dp.Sum != nil {
		sum = *dp.Sum
	}
	if dp.Count > 0 {
		avg = sum / float64(dp.Count)
	}
	return sum, avg
}

// orSlice returns s if non-nil, otherwise fallback.
func orSlice[T any](s []T, fallback []T) []T {
	if s != nil {
		return s
	}
	return fallback
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
