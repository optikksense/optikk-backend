package kafka

// OpenTelemetry semantic conventions for Kafka-related messaging telemetry.
// Canonical names come first; compatibility aliases preserve historical data.
//
// Attribute alias chains for `topic`, `consumer_group`, `operation_name`,
// `partition`, `messaging.system`, `node-id` are inlined directly in the SQL
// queries (see repository.go) — the SQL is the source of truth for which
// attribute names each kafka concept can come from.

const (
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
)

var (
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
