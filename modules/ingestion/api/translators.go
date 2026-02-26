package api

import (
	"strconv"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
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

// TranslateLogs converts OTLP logs payloads into our internal LogRecord format.
func TranslateLogs(teamUUID string, payload model.OTLPLogsPayload) []model.LogRecord {
	var logsToInsert []model.LogRecord

	for _, rl := range payload.ResourceLogs {
		rc := newResourceContext(rl.Resource.Attributes)

		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				logAttrs := otlpAttrMap(record.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, logAttrs)

				ts := nanosToTime(record.TimeUnixNano)
				if strings.TrimSpace(record.TimeUnixNano) == "" {
					ts = nanosToTime(record.ObservedTimeUnixNano)
				}

				level := strings.TrimSpace(record.SeverityText)
				if level == "" {
					level = severityTextFromNumber(record.SeverityNumber)
				}

				message := strings.TrimSpace(otlpAttrString(record.Body))
				if message == "" {
					message = strings.TrimSpace(logAttrs["message"])
				}

				infra := extractInfraLabels(logAttrs, rc.attrs)

				logsToInsert = append(logsToInsert, model.LogRecord{
					TeamUUID:   teamUUID,
					Timestamp:  ts,
					Level:      level,
					Service:    firstNonEmpty(logAttrs["service.name"], rc.serviceName),
					Logger:     firstNonEmpty(logAttrs["logger.name"], sl.Scope.Name),
					Message:    message,
					TraceID:    strings.TrimSpace(record.TraceID),
					SpanID:     strings.TrimSpace(record.SpanID),
					Host:       infra.host,
					Pod:        infra.pod,
					Container:  infra.container,
					Thread:     firstNonEmpty(logAttrs["thread.name"], logAttrs["thread.id"]),
					Exception:  firstNonEmpty(logAttrs["exception.message"], logAttrs["exception.type"]),
					Attributes: dbutil.JSONString(allAttrs),
				})
			}
		}
	}

	return logsToInsert
}

// TranslateSpans converts OTLP traces payloads into our internal SpanRecord format.
func TranslateSpans(teamUUID string, payload model.OTLPTracesPayload) []model.SpanRecord {
	var spansToInsert []model.SpanRecord

	for _, rs := range payload.ResourceSpans {
		rc := newResourceContext(rs.Resource.Attributes)

		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := otlpAttrMap(span.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, spanAttrs)

				startTime := nanosToTime(span.StartTimeUnixNano)
				endTime := nanosToTime(span.EndTimeUnixNano)
				durationMs := endTime.Sub(startTime).Milliseconds()
				if durationMs < 0 {
					durationMs = 0
				}

				status := "OK"
				statusMessage := ""
				if span.Status != nil {
					if span.Status.Code == 2 {
						status = "ERROR"
					}
					statusMessage = span.Status.Message
				}

				httpMethod := firstNonEmpty(spanAttrs["http.request.method"], spanAttrs["http.method"])
				httpURL := firstNonEmpty(spanAttrs["url.full"], spanAttrs["http.url"], spanAttrs["http.route"])
				httpStatusCode, _ := strconv.Atoi(firstNonEmpty(spanAttrs["http.response.status_code"], spanAttrs["http.status_code"]))

				infra := extractInfraLabels(spanAttrs, rc.attrs)

				rootInt := 0
				if span.ParentSpanID == "" {
					rootInt = 1
				}

				spansToInsert = append(spansToInsert, model.SpanRecord{
					TeamUUID:       teamUUID,
					TraceID:        span.TraceID,
					SpanID:         span.SpanID,
					ParentSpanID:   span.ParentSpanID,
					IsRoot:         rootInt,
					OperationName:  span.Name,
					ServiceName:    rc.serviceName,
					SpanKind:       spanKindString(span.Kind),
					StartTime:      startTime,
					EndTime:        endTime,
					DurationMs:     durationMs,
					Status:         status,
					StatusMessage:  statusMessage,
					HTTPMethod:     httpMethod,
					HTTPURL:        httpURL,
					HTTPStatusCode: httpStatusCode,
					Host:           infra.host,
					Pod:            infra.pod,
					Container:      infra.container,
					Attributes:     dbutil.JSONString(allAttrs),
				})
			}
		}
	}

	return spansToInsert
}
