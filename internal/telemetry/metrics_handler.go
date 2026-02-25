package telemetry

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// HandleMetrics is the Gin handler for POST /otlp/v1/metrics.
func (h *Handler) HandleMetrics(c *gin.Context) {
	teamUUID, ok := h.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	body, err := readBody(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload OTLPMetricsPayload
	if isProtobuf(c) {
		payload, err = protoToMetricsPayload(body)
	} else {
		err = json.Unmarshal(body, &payload)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP payload"})
		return
	}

	var metricsToInsert []MetricRecord

	for _, rm := range payload.ResourceMetrics {
		resourceAttrs := otlpAttrMap(rm.Resource.Attributes)
		serviceName := resourceAttrs["service.name"]
		if serviceName == "" {
			serviceName = "unknown"
		}

		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				category := metricCategory(metric.Name)

				switch {
				case metric.Gauge != nil:
					for _, dp := range metric.Gauge.DataPoints {
						dpAttrs := otlpAttrMap(dp.Attributes)
						labels := extractDPLabels(dpAttrs, resourceAttrs)
						v := numberDPValue(dp)
						ts := nanosToTime(dp.TimeUnixNano)
						allAttrs := mergeOTLPAttrs(resourceAttrs, dpAttrs)

						metricsToInsert = append(metricsToInsert, MetricRecord{
							TeamUUID:       teamUUID,
							MetricName:     metric.Name,
							MetricType:     "gauge",
							MetricCategory: category,
							ServiceName:    serviceName,
							Timestamp:      ts,
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
							Attributes:     dbutil.JSONString(allAttrs),
						})
					}

				case metric.Sum != nil:
					for _, dp := range metric.Sum.DataPoints {
						dpAttrs := otlpAttrMap(dp.Attributes)
						labels := extractDPLabels(dpAttrs, resourceAttrs)
						v := numberDPValue(dp)
						ts := nanosToTime(dp.TimeUnixNano)
						allAttrs := mergeOTLPAttrs(resourceAttrs, dpAttrs)

						metricsToInsert = append(metricsToInsert, MetricRecord{
							TeamUUID:       teamUUID,
							MetricName:     metric.Name,
							MetricType:     "sum",
							MetricCategory: category,
							ServiceName:    serviceName,
							Timestamp:      ts,
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
							Attributes:     dbutil.JSONString(allAttrs),
						})
					}

				case metric.Histogram != nil:
					for _, dp := range metric.Histogram.DataPoints {
						dpAttrs := otlpAttrMap(dp.Attributes)
						labels := extractDPLabels(dpAttrs, resourceAttrs)
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

						allAttrs := mergeOTLPAttrs(resourceAttrs, dpAttrs)
						if len(dp.BucketCounts) > 0 {
							allAttrs["_bucketCounts"] = dp.BucketCounts
							allAttrs["_explicitBounds"] = dp.ExplicitBounds
						}

						metricsToInsert = append(metricsToInsert, MetricRecord{
							TeamUUID:       teamUUID,
							MetricName:     metric.Name,
							MetricType:     "histogram",
							MetricCategory: category,
							ServiceName:    serviceName,
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
						})
					}
				}
			}
		}
	}

	if err := h.Repo.InsertMetrics(c.Request.Context(), metricsToInsert); err != nil {
		log.Printf("otlp: failed to insert metrics: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{"inserted": len(metricsToInsert)})
}
