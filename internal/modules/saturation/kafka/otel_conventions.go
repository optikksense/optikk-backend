package kafka

import "fmt"

// OpenTelemetry Semantic Conventions for Messaging Metrics (Kafka)
// Reference: https://opentelemetry.io/docs/specs/semconv/messaging/

const (
	// ── Messaging Attributes ──────────────────────────────────────────────
	AttrMessagingSystem                    = "messaging.system"
	AttrMessagingDestinationName           = "messaging.destination.name"
	AttrMessagingConsumerGroupName         = "messaging.consumer.group.name"
	AttrMessagingKafkaDestinationPartition = "messaging.kafka.destination.partition"
	AttrMessagingOperationName             = "messaging.operation.name"
	AttrErrorType                          = "error.type"
	AttrServerAddress                      = "server.address"

	// ── Produce metrics ───────────────────────────────────────────────────
	MetricPublishMessages    = "messaging.publish.messages"
	MetricPublishDuration    = "messaging.publish.duration"
	MetricClientSentMessages = "messaging.client.sent.messages"

	// ── Consume metrics ───────────────────────────────────────────────────
	MetricReceiveMessages        = "messaging.receive.messages"
	MetricReceiveDuration        = "messaging.receive.duration"
	MetricClientReceivedMessages = "messaging.client.received.messages"

	// ── Process metrics ───────────────────────────────────────────────────
	MetricProcessMessages = "messaging.process.messages"
	MetricProcessDuration = "messaging.process.duration"

	// ── Consumer lag ──────────────────────────────────────────────────────
	MetricKafkaConsumerLag    = "messaging.kafka.consumer.lag"
	MetricKafkaConsumerLagSum = "messaging.kafka.consumer.lag_sum"

	// ── Rebalance / health ────────────────────────────────────────────────
	MetricRebalanceCount       = "messaging.kafka.consumer.rebalance.count"
	MetricRebalanceDuration    = "messaging.kafka.consumer.rebalance.duration"
	MetricJoinCount            = "messaging.kafka.consumer.join.count"
	MetricSyncCount            = "messaging.kafka.consumer.sync.count"
	MetricHeartbeatCount       = "messaging.kafka.consumer.heartbeat.count"
	MetricFailedHeartbeatCount = "messaging.kafka.consumer.failed_heartbeat.count"
	MetricAssignedPartitions   = "messaging.kafka.consumer.assigned_partitions"

	// ── Client / broker ───────────────────────────────────────────────────
	MetricClientConnections       = "messaging.client.connections"
	MetricClientOperationDuration = "messaging.client.operation.duration"

	TableMetrics = "observability.metrics"
)

func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
