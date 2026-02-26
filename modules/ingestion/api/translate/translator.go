package translate

import "github.com/observability/observability-backend-go/modules/ingestion/model"

// Translator converts an OTLP payload of type P into a slice of domain records of type R.
type Translator[P any, R any] interface {
	Translate(teamUUID string, payload P) []R
}

// Pre-built translators for each signal type.
var (
	Spans   Translator[model.OTLPTracesPayload, model.SpanRecord]    = SpansTranslator{}
	Metrics Translator[model.OTLPMetricsPayload, model.MetricRecord] = MetricsTranslator{}
	Logs    Translator[model.OTLPLogsPayload, model.LogRecord]       = LogsTranslator{}
)
