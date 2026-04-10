package kafka

import "fmt"

// OpenTelemetry semantic conventions for Kafka-related messaging telemetry.
// Canonical names come first; compatibility aliases preserve historical data.
const (
	AttrMessagingSystem                    = "messaging.system"
	AttrMessagingDestinationName           = "messaging.destination.name"
	AttrMessagingConsumerGroupName         = "messaging.consumer.group.name"
	AttrMessagingKafkaDestinationPartition = "messaging.kafka.destination.partition"
	AttrMessagingOperationName             = "messaging.operation.name"
	AttrErrorType                          = "error.type"
	AttrServerAddress                      = "server.address"

	MetricPublishMessages         = "messaging.publish.messages"
	MetricPublishDuration         = "messaging.publish.duration"
	MetricClientSentMessages      = "messaging.client.sent.messages"
	MetricReceiveMessages         = "messaging.receive.messages"
	MetricReceiveDuration         = "messaging.receive.duration"
	MetricClientReceivedMessages  = "messaging.client.received.messages"
	MetricProcessMessages         = "messaging.process.messages"
	MetricProcessDuration         = "messaging.process.duration"
	MetricKafkaConsumerLag        = "messaging.kafka.consumer.lag"
	MetricKafkaConsumerLagSum     = "messaging.kafka.consumer.lag_sum"
	MetricRebalanceCount          = "messaging.kafka.consumer.rebalance.count"
	MetricRebalanceDuration       = "messaging.kafka.consumer.rebalance.duration"
	MetricJoinCount               = "messaging.kafka.consumer.join.count"
	MetricSyncCount               = "messaging.kafka.consumer.sync.count"
	MetricHeartbeatCount          = "messaging.kafka.consumer.heartbeat.count"
	MetricFailedHeartbeatCount    = "messaging.kafka.consumer.failed_heartbeat.count"
	MetricAssignedPartitions      = "messaging.kafka.consumer.assigned_partitions"
	MetricClientConnections       = "messaging.client.connections"
	MetricClientOperationDuration = "messaging.client.operation.duration"

	TableMetrics = "observability.metrics"
)

var (
	topicAttributeAliases = []string{
		"topic",
		AttrMessagingDestinationName,
		"messaging.destination",
		"messaging.kafka.destination.name",
	}

	consumerGroupAttributeAliases = []string{
		"client-id",
		AttrMessagingConsumerGroupName,
		"messaging.kafka.consumer.group",
		"messaging.consumer.group",
	}

	operationAttributeAliases = []string{
		AttrMessagingOperationName,
		"messaging.operation",
		"messaging.client.operation.name",
	}

	partitionAttributeAliases = []string{
		AttrMessagingKafkaDestinationPartition,
		"messaging.kafka.partition",
	}

	producerMetricAliases = []string{
		MetricPublishMessages,
		MetricClientSentMessages,
		"kafka.producer.message.count",
		"messaging.client.published.messages",
	}

	consumerMetricAliases = []string{
		MetricReceiveMessages,
		MetricClientReceivedMessages,
		"kafka.consumer.message.count",
		"messaging.client.consumed.messages",
	}

	processMetricAliases = []string{
		MetricProcessMessages,
	}

	consumerLagMetricAliases = []string{
		MetricKafkaConsumerLag,
		MetricKafkaConsumerLagSum,
		"kafka.consumer.lag",
		"queue.depth",
	}

	rebalanceMetricAliases = []string{
		MetricRebalanceCount,
		MetricJoinCount,
		MetricSyncCount,
		MetricHeartbeatCount,
		MetricFailedHeartbeatCount,
		MetricAssignedPartitions,
	}

	durationMetricAliases = []string{
		MetricPublishDuration,
		MetricReceiveDuration,
		MetricProcessDuration,
		MetricClientOperationDuration,
	}

	publishOperationAliases = []string{"publish", "produce", "send"}
	receiveOperationAliases = []string{"receive", "consume"}
	processOperationAliases = []string{"process"}
)

func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
