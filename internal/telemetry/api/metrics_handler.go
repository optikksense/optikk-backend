package api

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/telemetry/model"
)

// HandleMetrics is the Gin handler for POST /otlp/v1/metrics.
func (h *Handler) HandleMetrics(c *gin.Context) {
	teamUUID, payload, ok := decodeRequest(h, c, protoToMetricsPayload)
	if !ok {
		return
	}

	var metricsToInsert []model.MetricRecord

	for _, rm := range payload.ResourceMetrics {
		rc := newResourceContext(rm.Resource.Attributes)

		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				category := metricCategory(metric.Name)

				switch {
				case metric.Gauge != nil:
					for _, dp := range metric.Gauge.DataPoints {
						metricsToInsert = append(metricsToInsert,
							buildNumberMetricRecord(teamUUID, rc, metric.Name, "gauge", category, dp))
					}

				case metric.Sum != nil:
					for _, dp := range metric.Sum.DataPoints {
						metricsToInsert = append(metricsToInsert,
							buildNumberMetricRecord(teamUUID, rc, metric.Name, "sum", category, dp))
					}

				case metric.Histogram != nil:
					for _, dp := range metric.Histogram.DataPoints {
						metricsToInsert = append(metricsToInsert,
							buildHistogramMetricRecord(teamUUID, rc, metric.Name, category, dp))
					}
				}
			}
		}
	}

	if len(metricsToInsert) > 0 {
		if err := h.Ingester.IngestMetrics(c.Request.Context(), metricsToInsert); err != nil {
			log.Printf("otlp: failed to ingest %d metrics: %v", len(metricsToInsert), err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest metrics"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(metricsToInsert)})
}

// buildNumberMetricRecord creates a MetricRecord from a gauge or sum data point.
func buildNumberMetricRecord(teamUUID string, rc resourceContext, name, metricType, category string, dp model.OTLPNumberDataPoint) model.MetricRecord {
	dpAttrs := otlpAttrMap(dp.Attributes)
	labels := extractDPLabels(dpAttrs, rc.attrs)
	v := numberDPValue(dp)

	return model.MetricRecord{
		TeamUUID:       teamUUID,
		MetricName:     name,
		MetricType:     metricType,
		MetricCategory: category,
		ServiceName:    rc.serviceName,
		Timestamp:      nanosToTime(dp.TimeUnixNano),
		Value:          v,
		Count:          1,
		Sum:            v,
		Min:            v,
		Max:            v,
		Avg:            v,
		HTTPMethod:     labels.httpMethod,
		HTTPStatusCode: labels.httpStatusCode,
		Status:         labels.status,
		Host:           labels.host,
		Pod:            labels.pod,
		Container:      labels.container,
		Attributes:     dbutil.JSONString(mergeOTLPAttrs(rc.attrs, dpAttrs)),
	}
}

// buildHistogramMetricRecord creates a MetricRecord from a histogram data point.
func buildHistogramMetricRecord(teamUUID string, rc resourceContext, name, category string, dp model.OTLPHistogramDataPoint) model.MetricRecord {
	dpAttrs := otlpAttrMap(dp.Attributes)
	labels := extractDPLabels(dpAttrs, rc.attrs)
	ts := nanosToTime(dp.TimeUnixNano)

	count, _ := strconv.ParseInt(dp.Count, 10, 64)
	sumVal := 0.0
	if dp.Sum != nil {
		sumVal = *dp.Sum
	}
	minVal := 0.0
	if dp.Min != nil {
		minVal = *dp.Min
	}
	maxVal := 0.0
	if dp.Max != nil {
		maxVal = *dp.Max
	}
	avgVal := 0.0
	if count > 0 {
		avgVal = sumVal / float64(count)
	}

	p50Val := minVal
	p99Val := maxVal
	p95Val := minVal + (maxVal-minVal)*0.85
	if minVal == 0 && maxVal == 0 {
		p50Val = avgVal
		p95Val = avgVal
		p99Val = avgVal
	}

	allAttrs := mergeOTLPAttrs(rc.attrs, dpAttrs)
	if len(dp.BucketCounts) > 0 {
		allAttrs["_bucketCounts"] = dp.BucketCounts
		allAttrs["_explicitBounds"] = dp.ExplicitBounds
	}

	return model.MetricRecord{
		TeamUUID:       teamUUID,
		MetricName:     name,
		MetricType:     "histogram",
		MetricCategory: category,
		ServiceName:    rc.serviceName,
		Timestamp:      ts,
		Value:          avgVal,
		Count:          count,
		Sum:            sumVal,
		Min:            minVal,
		Max:            maxVal,
		Avg:            avgVal,
		P50:            p50Val,
		P95:            p95Val,
		P99:            p99Val,
		HTTPMethod:     labels.httpMethod,
		HTTPStatusCode: labels.httpStatusCode,
		Status:         labels.status,
		Host:           labels.host,
		Pod:            labels.pod,
		Container:      labels.container,
		Attributes:     dbutil.JSONString(allAttrs),
	}
}
