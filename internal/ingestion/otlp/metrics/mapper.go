package metrics

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/protoconv"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// MetricRow structurally maps an OTLP metric to ClickHouse columns.
type MetricRow struct {
	TeamID              uint32            `ch:"team_id"`
	Env                 string            `ch:"env"`
	MetricName          string            `ch:"metric_name"`
	MetricType          string            `ch:"metric_type"`
	Temporality         string            `ch:"temporality"`
	IsMonotonic         bool              `ch:"is_monotonic"`
	Unit                string            `ch:"unit"`
	Description         string            `ch:"description"`
	ResourceFingerprint uint64            `ch:"resource_fingerprint"`
	Timestamp           time.Time         `ch:"timestamp"`
	Value               float64           `ch:"value"`
	HistSum             float64           `ch:"hist_sum"`
	HistCount           uint64            `ch:"hist_count"`
	HistBuckets         []float64         `ch:"hist_buckets"`
	HistCounts          []uint64          `ch:"hist_counts"`
	Attributes          map[string]string `ch:"attributes"`
}

type metricHeader struct {
	teamID      uint32
	env         string
	fingerprint uint64
	resMap      map[string]string
}

// mapMetrics converts an OTLP metrics export request into internal Protobuf messages for Kafka transport.
func mapMetrics(teamID int64, req *metricspb.ExportMetricsServiceRequest) []*proto.MetricRow {
	var rows []*proto.MetricRow
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

// envFromResource returns the deployment environment, or empty string if not set.
func envFromResource(resMap map[string]string) string {
	if env := resMap["deployment.environment"]; env != "" {
		return env
	}
	return ""
}

// mapGaugeRows appends one proto row per Gauge data point.
func mapGaugeRows(rows []*proto.MetricRow, hdr metricHeader, m *metricsdatapb.Metric, dps []*metricsdatapb.NumberDataPoint) []*proto.MetricRow {
	for _, dp := range dps {
		rows = append(rows, (&MetricRow{
			TeamID:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.Name,
			MetricType:          "Gauge",
			Temporality:         "Unspecified",
			IsMonotonic:         false,
			Unit:                m.Unit,
			Description:         m.Description,
			ResourceFingerprint: hdr.fingerprint,
			Timestamp:           protoconv.NanoToTime(dp.TimeUnixNano),
			Value:               numberDataPointValue(dp),
			HistBuckets:         emptyBounds,
			HistCounts:          emptyCounts,
			Attributes:          protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes),
		}).ToProto())
	}
	return rows
}

// mapSumRows appends one proto row per Sum data point.
func mapSumRows(rows []*proto.MetricRow, hdr metricHeader, m *metricsdatapb.Metric, sum *metricsdatapb.Sum) []*proto.MetricRow {
	temporality := temporalityString(sum.AggregationTemporality)
	for _, dp := range sum.DataPoints {
		rows = append(rows, (&MetricRow{
			TeamID:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.Name,
			MetricType:          "Sum",
			Temporality:         temporality,
			IsMonotonic:         sum.IsMonotonic,
			Unit:                m.Unit,
			Description:         m.Description,
			ResourceFingerprint: hdr.fingerprint,
			Timestamp:           protoconv.NanoToTime(dp.TimeUnixNano),
			Value:               numberDataPointValue(dp),
			HistBuckets:         emptyBounds,
			HistCounts:          emptyCounts,
			Attributes:          protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes),
		}).ToProto())
	}
	return rows
}

// mapHistogramRows appends one proto row per Histogram data point.
func mapHistogramRows(rows []*proto.MetricRow, hdr metricHeader, m *metricsdatapb.Metric, hist *metricsdatapb.Histogram) []*proto.MetricRow {
	temporality := temporalityString(hist.AggregationTemporality)
	for _, dp := range hist.DataPoints {
		sum, avg := histValues(dp)
		rows = append(rows, (&MetricRow{
			TeamID:              hdr.teamID,
			Env:                 hdr.env,
			MetricName:          m.Name,
			MetricType:          "Histogram",
			Temporality:         temporality,
			IsMonotonic:         false,
			Unit:                m.Unit,
			Description:         m.Description,
			ResourceFingerprint: hdr.fingerprint,
			Timestamp:           protoconv.NanoToTime(dp.TimeUnixNano),
			Value:               avg,
			HistSum:             sum,
			HistCount:           dp.Count,
			HistBuckets:         orSlice(dp.ExplicitBounds, emptyBounds),
			HistCounts:          orSlice(dp.BucketCounts, emptyCounts),
			Attributes:          protoconv.MergeAttrsMap(hdr.resMap, dp.Attributes),
		}).ToProto())
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

func (row *MetricRow) ToProto() *proto.MetricRow {
	return &proto.MetricRow{
		TeamID:              row.TeamID,
		Env:                 row.Env,
		MetricName:          row.MetricName,
		MetricType:          row.MetricType,
		Temporality:         row.Temporality,
		IsMonotonic:         row.IsMonotonic,
		Unit:                row.Unit,
		Description:         row.Description,
		ResourceFingerprint: row.ResourceFingerprint,
		Timestamp:           timestamppb.New(row.Timestamp),
		Value:               row.Value,
		HistSum:             row.HistSum,
		HistCount:           row.HistCount,
		HistBuckets:         row.HistBuckets,
		HistCounts:          row.HistCounts,
		Attributes:          row.Attributes,
	}
}

func FromProto(p *proto.MetricRow) *MetricRow {
	return &MetricRow{
		TeamID:              p.TeamID,
		Env:                 p.Env,
		MetricName:          p.MetricName,
		MetricType:          p.MetricType,
		Temporality:         p.Temporality,
		IsMonotonic:         p.IsMonotonic,
		Unit:                p.Unit,
		Description:         p.Description,
		ResourceFingerprint: p.ResourceFingerprint,
		Timestamp:           p.Timestamp.AsTime(),
		Value:               p.Value,
		HistSum:             p.HistSum,
		HistCount:           p.HistCount,
		HistBuckets:         p.HistBuckets,
		HistCounts:          p.HistCounts,
		Attributes:          p.Attributes,
	}
}
