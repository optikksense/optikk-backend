package api

import (
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// TranslateMetrics converts OTLP metrics payloads into our internal MetricRecord format.
func TranslateMetrics(teamUUID string, payload model.OTLPMetricsPayload) []model.MetricRecord {
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

	return metricsToInsert
}
