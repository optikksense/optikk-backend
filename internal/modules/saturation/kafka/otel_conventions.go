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

	// Metric Names
	MetricMessagingConsumerLag = "messaging.consumer.lag"
	MetricMessagingPublishSize = "messaging.publish.message.size"
	MetricMessagingReceiveSize = "messaging.receive.message.size"

	TableMetrics = "metrics"
)

// attrString returns a CH 26+ native JSON path expression that reads a String
// from the attributes JSON column. Replaces JSONExtractString(attributes, 'key').
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
