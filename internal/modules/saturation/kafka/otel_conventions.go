package kafka

import "fmt"

// OpenTelemetry Semantic Conventions for Messaging Metrics (Kafka)
// Reference: https://opentelemetry.io/docs/specs/semconv/messaging/

const (
	// Messaging Attributes
	AttrMessagingSystem                    = "messaging.system"
	AttrMessagingDestinationName           = "messaging.destination.name"
	AttrMessagingOperation                 = "messaging.operation"
	AttrMessagingKafkaConsumerGroup        = "messaging.kafka.consumer.group"
	AttrMessagingKafkaDestinationPartition = "messaging.kafka.destination.partition"
	AttrMessagingError                     = "error"

	// Span Types & Categories
	SpanKindProducer = "SPAN_KIND_PRODUCER"
	SpanKindConsumer = "SPAN_KIND_CONSUMER"

	// Metric Names
	MetricMessagingConsumerLag = "messaging.consumer.lag"
	MetricMessagingPublishSize = "messaging.publish.message.size"
	MetricMessagingReceiveSize = "messaging.receive.message.size"

	// Table column names for queries
	ColTraceID        = "TraceId"
	ColSpanID         = "SpanId"
	ColSpanName       = "SpanName"
	ColDuration       = "Duration"
	ColSpanStatus     = "StatusCode"
	ColSpanAttributes = "SpanAttributes"

	// Status Values
	StatusOk    = "STATUS_CODE_OK"
	StatusError = "STATUS_CODE_ERROR"
	StatusUnset = "STATUS_CODE_UNSET"

	TableSpans   = "otel_traces"
	TableMetrics = "otel_metrics"
)

// ExtractJSONString builds a ClickHouse JSON extraction expression.
func ExtractJSONString(column, key string) string {
	return fmt.Sprintf("JSONExtractString(%s, '%s')", column, key)
}

// QueueSystemCondition builds the condition to verify a span relates to a queue processing.
func QueueSystemCondition() string {
	messagingSystemAttr := ExtractJSONString(ColSpanAttributes, AttrMessagingSystem)
	return fmt.Sprintf("length(%s) > 0 AND %s = '%s'",
		messagingSystemAttr,
		messagingSystemAttr,
		"kafka",
	)
}
