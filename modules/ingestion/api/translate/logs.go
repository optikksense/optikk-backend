package translate

import (
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// LogsTranslator converts OTLP logs payloads into LogRecord slices.
type LogsTranslator struct{}

func (LogsTranslator) Translate(teamUUID string, payload model.OTLPLogsPayload) []model.LogRecord {
	var logs []model.LogRecord

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

				logs = append(logs, model.LogRecord{
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

	return logs
}
