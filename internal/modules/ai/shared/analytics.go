package shared

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

// analyticsAggregated is a single GROUP BY output row before sorting.
// Kept package-level so computeAnalyticsResult and sortAnalytics can share it.
type analyticsAggregated struct {
	dims       []string
	timeBucket int64
	metrics    []float64
}

// computeAnalyticsResult reproduces the explorer/analytics GROUP BY + aggregate
// semantics entirely in Go over pre-derived AIRunDetail rows. This avoids
// emitting any of the banned SQL expressions (quantileTDigest / toString /
// toFloat64 / avg / if) that the builder package generates when pointed at a
// ClickHouse table.
func computeAnalyticsResult(req analytics.AnalyticsRequest, rows []AIRunDetail) *analytics.AnalyticsResult {
	isTimeseries := req.VizMode == analytics.VizTimeseries && req.Step != ""
	stepSec := stepToSeconds(req.Step)

	columns := make([]string, 0, 1+len(req.GroupBy)+len(req.Aggregations))
	if isTimeseries {
		columns = append(columns, "time_bucket")
	}
	columns = append(columns, req.GroupBy...)
	for _, a := range req.Aggregations {
		columns = append(columns, a.Alias)
	}

	type bucketKey struct {
		timeBucket int64
		dims       string
	}
	type bucket struct {
		timeBucket int64
		dimValues  []string
		rows       []AIRunDetail
	}
	buckets := make(map[bucketKey]*bucket)
	order := make([]bucketKey, 0)

	for i := range rows {
		dimValues := make([]string, 0, len(req.GroupBy))
		for _, dim := range req.GroupBy {
			dimValues = append(dimValues, dimensionValue(dim, rows[i]))
		}
		var ts int64
		if isTimeseries {
			ts = bucketStart(rows[i].StartTime, stepSec)
		}
		key := bucketKey{timeBucket: ts, dims: strings.Join(dimValues, "\x1f")}
		b, ok := buckets[key]
		if !ok {
			b = &bucket{timeBucket: ts, dimValues: dimValues}
			buckets[key] = b
			order = append(order, key)
		}
		b.rows = append(b.rows, rows[i])
	}

	aggList := make([]analyticsAggregated, 0, len(order))
	for _, k := range order {
		b := buckets[k]
		metrics := make([]float64, 0, len(req.Aggregations))
		for _, a := range req.Aggregations {
			metrics = append(metrics, applyAggregation(a, b.rows))
		}
		aggList = append(aggList, analyticsAggregated{
			dims:       b.dimValues,
			timeBucket: b.timeBucket,
			metrics:    metrics,
		})
	}

	sortAnalytics(req, aggList, isTimeseries)

	limit := req.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if len(aggList) > limit {
		aggList = aggList[:limit]
	}

	resultRows := make([]analytics.AnalyticsRow, 0, len(aggList))
	for _, a := range aggList {
		cells := make([]analytics.AnalyticsCell, 0, len(columns))
		if isTimeseries {
			ts := time.Unix(a.timeBucket, 0).UTC().Format("2006-01-02 15:04:05")
			v := ts
			cells = append(cells, analytics.AnalyticsCell{
				Key:         "time_bucket",
				Type:        analytics.ValueString,
				StringValue: &v,
			})
		}
		for i, dim := range req.GroupBy {
			cells = append(cells, cellFromDimensionValue(dim, a.dims[i]))
		}
		for i, aggDef := range req.Aggregations {
			v := a.metrics[i]
			if math.IsNaN(v) || math.IsInf(v, 0) {
				v = 0
			}
			rounded := math.Round(v*100) / 100
			cells = append(cells, analytics.AnalyticsCell{
				Key:         aggDef.Alias,
				Type:        analytics.ValueNumber,
				NumberValue: &rounded,
			})
		}
		resultRows = append(resultRows, analytics.AnalyticsRow{Cells: cells})
	}

	return &analytics.AnalyticsResult{Columns: columns, Rows: resultRows}
}

func dimensionValue(dim string, row AIRunDetail) string {
	switch dim {
	case "provider":
		return row.Provider
	case "service", "service_name":
		return row.ServiceName
	case "model", "request_model":
		return row.RequestModel
	case "operation":
		return row.Operation
	case "prompt_template":
		return row.PromptTemplate
	case "session_id":
		return row.SessionID
	case "conversation_id":
		return row.ConversationID
	case "finish_reason":
		return row.FinishReason
	case "guardrail_state":
		if row.GuardrailState == "" {
			return "not_evaluated"
		}
		return row.GuardrailState
	case "quality_bucket":
		return row.QualityBucket
	case "status":
		return row.Status
	default:
		return ""
	}
}

func applyAggregation(a analytics.Aggregation, rows []AIRunDetail) float64 {
	switch a.Function {
	case "count":
		return float64(len(rows))
	case "count_unique":
		seen := make(map[float64]struct{}, len(rows))
		seenStr := make(map[string]struct{}, len(rows))
		for i := range rows {
			if v, ok := numericAggField(a.Field, rows[i]); ok {
				seen[v] = struct{}{}
			} else if s, ok := stringAggField(a.Field, rows[i]); ok {
				seenStr[s] = struct{}{}
			}
		}
		if len(seen) > 0 && len(seenStr) > 0 {
			return float64(len(seen) + len(seenStr))
		}
		if len(seen) > 0 {
			return float64(len(seen))
		}
		return float64(len(seenStr))
	case "sum":
		var s float64
		for i := range rows {
			if v, ok := numericAggField(a.Field, rows[i]); ok {
				s += v
			}
		}
		return s
	case "min":
		first := true
		var m float64
		for i := range rows {
			if v, ok := numericAggField(a.Field, rows[i]); ok {
				if first || v < m {
					m = v
					first = false
				}
			}
		}
		return m
	case "max":
		var m float64
		for i := range rows {
			if v, ok := numericAggField(a.Field, rows[i]); ok {
				if v > m {
					m = v
				}
			}
		}
		return m
	case "avg":
		var s float64
		var c int
		for i := range rows {
			if v, ok := numericAggField(a.Field, rows[i]); ok {
				s += v
				c++
			}
		}
		if c == 0 {
			return 0
		}
		return s / float64(c)
	case "p50":
		return percentile(a.Field, rows, 0.5)
	case "p75":
		return percentile(a.Field, rows, 0.75)
	case "p90":
		return percentile(a.Field, rows, 0.9)
	case "p95":
		return percentile(a.Field, rows, 0.95)
	case "p99":
		return percentile(a.Field, rows, 0.99)
	}
	return 0
}

func percentile(field string, rows []AIRunDetail, p float64) float64 {
	values := make([]float64, 0, len(rows))
	for i := range rows {
		if v, ok := numericAggField(field, rows[i]); ok {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	rank := p * float64(len(values)-1)
	idx := int(math.Round(rank))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func numericAggField(field string, row AIRunDetail) (float64, bool) {
	switch field {
	case "latency_ms":
		return row.LatencyMs, true
	case "ttft_ms":
		return row.TTFTMs, true
	case "input_tokens":
		return row.InputTokens, true
	case "output_tokens":
		return row.OutputTokens, true
	case "total_tokens":
		return row.TotalTokens, true
	case "cost_usd":
		return row.CostUSD, true
	case "quality_score":
		return row.QualityScore, true
	}
	return 0, false
}

func stringAggField(field string, row AIRunDetail) (string, bool) {
	switch field {
	case "provider":
		return row.Provider, true
	case "service", "service_name":
		return row.ServiceName, true
	case "model", "request_model":
		return row.RequestModel, true
	case "operation":
		return row.Operation, true
	case "prompt_template":
		return row.PromptTemplate, true
	case "session_id":
		return row.SessionID, true
	case "conversation_id":
		return row.ConversationID, true
	case "finish_reason":
		return row.FinishReason, true
	}
	return "", false
}

func cellFromDimensionValue(dim, value string) analytics.AnalyticsCell {
	_ = dim
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return analytics.AnalyticsCell{Key: dim, Type: analytics.ValueInteger, IntegerValue: &i}
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		rounded := math.Round(f*100) / 100
		return analytics.AnalyticsCell{Key: dim, Type: analytics.ValueNumber, NumberValue: &rounded}
	}
	v := value
	return analytics.AnalyticsCell{Key: dim, Type: analytics.ValueString, StringValue: &v}
}

func sortAnalytics(req analytics.AnalyticsRequest, rows []analyticsAggregated, isTimeseries bool) {
	dir := strings.ToUpper(req.OrderDir)
	asc := dir == "ASC"

	var metricIdx int = -1
	if req.OrderBy != "" {
		for i, a := range req.Aggregations {
			if a.Alias == req.OrderBy {
				metricIdx = i
				break
			}
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if isTimeseries {
			if rows[i].timeBucket != rows[j].timeBucket {
				return rows[i].timeBucket < rows[j].timeBucket
			}
		}
		if metricIdx >= 0 && metricIdx < len(rows[i].metrics) {
			if asc {
				return rows[i].metrics[metricIdx] < rows[j].metrics[metricIdx]
			}
			return rows[i].metrics[metricIdx] > rows[j].metrics[metricIdx]
		}
		// Default: first metric descending.
		if len(rows[i].metrics) > 0 && len(rows[j].metrics) > 0 {
			if asc {
				return rows[i].metrics[0] < rows[j].metrics[0]
			}
			return rows[i].metrics[0] > rows[j].metrics[0]
		}
		return false
	})
}
