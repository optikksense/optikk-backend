package saturation

import "strings"

// OpenTelemetry Semantic Conventions for Saturation Metrics
// Based on OpenTelemetry Semantic Conventions for Messaging and Database
// Reference: https://opentelemetry.io/docs/specs/semconv/messaging/
// Reference: https://opentelemetry.io/docs/specs/semconv/database/

const (
	// Metrics Table Columns
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColMetricName  = "metric_name"
	ColValue       = "value"
	ColServiceName = "service_name"
	ColAttributes  = "attributes"
	ColCount       = "count"
	ColAvg         = "avg"
	ColMax         = "max"
	ColMin         = "min"
	ColP95         = "p95"

	// Messaging Attributes - OpenTelemetry Semantic Conventions
	AttrMessagingDestinationName = "messaging.destination.name"
	AttrMessagingSourceName      = "messaging.source.name"
	AttrMessagingDestination     = "messaging.destination"
	AttrMessagingQueueName       = "messaging.queue.name"
	AttrMessagingSystem          = "messaging.system"

	// Kafka-specific Attributes (OpenTelemetry)
	AttrMessagingKafkaDestination = "messaging.kafka.destination"
	AttrMessagingKafkaTopic       = "messaging.kafka.topic"

	// Vendor-specific Kafka Attributes
	AttrKafkaTopic = "kafka.topic"
	AttrTopic      = "topic"
	AttrQueueName  = "queue.name"
	AttrName       = "name"

	// Database Attributes - OpenTelemetry Semantic Conventions
	AttrDBSystem         = "db.system"
	AttrDBCollectionName = "db.collection.name"
	AttrDBSQLTable       = "db.sql.table"
	AttrDBStatement      = "db.statement"

	// MongoDB-specific Attributes
	AttrDBMongoDBCollection = "db.mongodb.collection"

	// Vendor-specific DB Attributes
	AttrCollection = "collection"
	AttrPool       = "pool"

	// Kafka Consumer Lag Metrics (OpenTelemetry)
	MetricMessagingKafkaConsumerLag           = "messaging.kafka.consumer.lag"
	MetricMessagingKafkaConsumerRecordsLag    = "messaging.kafka.consumer.records.lag"
	MetricMessagingKafkaConsumerRecordsLagMax = "messaging.kafka.consumer.records.lag.max"

	// Vendor-specific Kafka Metrics
	MetricKafkaConsumerLag                              = "kafka.consumer.lag"
	MetricKafkaConsumerRecordsLag                       = "kafka.consumer.records.lag"
	MetricKafkaConsumerRecordsLagDash                   = "kafka.consumer.records-lag" // Alternative format with dash
	MetricKafkaConsumerRecordsLagMax                    = "kafka.consumer.records.lag.max"
	MetricKafkaConsumerFetchManagerRecordsLag           = "kafka.consumer.fetch.manager.records.lag"
	MetricKafkaConsumerFetchManagerRecordsLagMax        = "kafka.consumer.fetch.manager.records.lag.max"
	MetricKafkaConsumerFetchRecordsLag                  = "kafka.consumer.fetch.records.lag"
	MetricKafkaConsumerFetchRecordsLagMax               = "kafka.consumer.fetch.records.lag.max"
	MetricKafkaProducerRecordSendTotal                  = "kafka.producer.record.send.total"
	MetricKafkaConsumerFetchManagerRecordsConsumedTotal = "kafka.consumer.fetch.manager.records.consumed.total"

	// OpenTelemetry Kafka Metrics (with dash format)
	MetricMessagingKafkaConsumerRecordsLagDash = "messaging.kafka.consumer.records-lag" // Alternative format with dash

	// Queue Metrics
	MetricQueueDepth          = "queue.depth"
	MetricMessagingQueueDepth = "messaging.queue.depth"
	MetricExecutorQueued      = "executor.queued"

	// Application-specific Kafka Metrics
	MetricMessagingKafkaPublished   = "messaging.kafka.published"
	MetricMessagingKafkaConsumed    = "messaging.kafka.consumed"
	MetricAppActivityKafkaPublished = "app.activity.kafka.published"
	MetricAppActivityKafkaConsumed  = "app.activity.kafka.consumed"

	// Spring Kafka Metrics (vendor-specific)
	MetricSpringKafkaTemplate = "spring.kafka.template"
	MetricSpringKafkaListener = "spring.kafka.listener"

	// Database Metrics
	MetricMongoDBDriverCommands      = "mongodb.driver.commands"
	MetricHikariCPConnectionsUsage   = "hikaricp.connections.usage"
	MetricHikariCPConnectionsAcquire = "hikaricp.connections.acquire"
	MetricHikariCPConnectionsActive  = "hikaricp.connections.active"
	MetricHikariCPConnectionsMax     = "hikaricp.connections.max"
	MetricJDBCConnectionsActive      = "jdbc.connections.active"
	MetricJDBCConnectionsMax         = "jdbc.connections.max"

	// Cache Metrics
	MetricCacheHits   = "cache.hits"
	MetricCacheMisses = "cache.misses"

	// Replication Metrics
	MetricDBReplicationLagMs = "db.replication.lag.ms"
	MetricDBClientErrors     = "db.client.errors"

	// Messaging System Values
	MessagingSystemKafka = "kafka"

	// Database System Values
	DBSystemMongoDB    = "mongodb"
	DBSystemMySQL      = "mysql"
	DBSystemPostgreSQL = "postgres"
	DBSystemRedis      = "redis"
	DBSystemSQL        = "sql"

	// Default Values
	DefaultUnknown = "unknown"

	// Time Bucketing Intervals (in milliseconds)
	ThreeHours      = 3 * 3_600_000
	TwentyFourHours = 24 * 3_600_000
	OneWeek         = 168 * 3_600_000

	// Time Bucket Intervals (in seconds)
	BucketOneMinute   = 60
	BucketFiveMinutes = 300
	BucketOneHour     = 3600
	BucketOneDay      = 86400

	// Time Format
	TimeFormatISO8601 = "%Y-%m-%dT%H:%i:%SZ"

	// Query Limits
	MaxTopQueues = 50
	MaxTopTables = 50
)

// Metric Sets - Grouped metrics for easier querying
var (
	// KafkaConsumerLagMetrics - All metrics related to Kafka consumer lag
	KafkaConsumerLagMetrics = []string{
		MetricMessagingKafkaConsumerLag,
		MetricMessagingKafkaConsumerRecordsLag,
		MetricMessagingKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerLag,
		MetricKafkaConsumerRecordsLag,
		MetricKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerFetchManagerRecordsLag,
		MetricKafkaConsumerFetchManagerRecordsLagMax,
		MetricKafkaConsumerFetchRecordsLagMax,
	}

	// KafkaConsumerLagMetricsExtended - All consumer lag metrics including dash variants and executor
	KafkaConsumerLagMetricsExtended = []string{
		MetricMessagingKafkaConsumerLag,
		MetricMessagingKafkaConsumerRecordsLag,
		MetricMessagingKafkaConsumerRecordsLagDash,
		MetricMessagingKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerLag,
		MetricKafkaConsumerRecordsLag,
		MetricKafkaConsumerRecordsLagDash,
		MetricKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerFetchManagerRecordsLag,
		MetricKafkaConsumerFetchManagerRecordsLagMax,
		MetricKafkaConsumerFetchRecordsLag,
		MetricKafkaConsumerFetchRecordsLagMax,
		MetricExecutorQueued,
	}

	// QueueDepthMetrics - All queue depth metrics
	QueueDepthMetrics = []string{
		MetricQueueDepth,
		MetricMessagingQueueDepth,
		MetricExecutorQueued,
	}

	// KafkaProducerMetrics - All metrics related to Kafka producer
	KafkaProducerMetrics = []string{
		MetricKafkaProducerRecordSendTotal,
		MetricSpringKafkaTemplate,
	}

	// KafkaConsumerMetrics - All metrics related to Kafka consumer consumption
	KafkaConsumerMetrics = []string{
		MetricSpringKafkaListener,
		MetricKafkaConsumerFetchManagerRecordsConsumedTotal,
	}

	// AllQueueMetrics - All queue-related metrics for top queues analysis
	AllQueueMetrics = []string{
		MetricQueueDepth,
		MetricMessagingQueueDepth,
		MetricExecutorQueued,
		MetricSpringKafkaTemplate,
		MetricSpringKafkaListener,
		MetricMessagingKafkaConsumerLag,
		MetricMessagingKafkaConsumerRecordsLag,
		MetricMessagingKafkaConsumerRecordsLagDash,
		MetricMessagingKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerLag,
		MetricKafkaConsumerRecordsLag,
		MetricKafkaConsumerRecordsLagDash,
		MetricKafkaConsumerRecordsLagMax,
		MetricKafkaConsumerFetchManagerRecordsLag,
		MetricKafkaConsumerFetchManagerRecordsLagMax,
		MetricKafkaConsumerFetchRecordsLag,
		MetricKafkaConsumerFetchRecordsLagMax,
		MetricMessagingKafkaPublished,
		MetricMessagingKafkaConsumed,
		MetricAppActivityKafkaPublished,
		MetricAppActivityKafkaConsumed,
	}

	// DatabaseLatencyMetrics - Metrics for database query latency
	DatabaseLatencyMetrics = []string{
		MetricMongoDBDriverCommands,
		MetricHikariCPConnectionsUsage,
	}

	// DatabaseAllMetrics - All database-related metrics
	DatabaseAllMetrics = []string{
		MetricMongoDBDriverCommands,
		MetricHikariCPConnectionsAcquire,
		MetricHikariCPConnectionsUsage,
		MetricHikariCPConnectionsActive,
		MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive,
		MetricJDBCConnectionsMax,
		MetricCacheHits,
		MetricCacheMisses,
		MetricDBReplicationLagMs,
		MetricDBClientErrors,
	}

	// CacheMetrics - Cache hit/miss metrics
	CacheMetrics = []string{
		MetricCacheHits,
		MetricCacheMisses,
	}
)

// TimeBucketExpression returns the appropriate time bucket expression based on time range
func TimeBucketExpression(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "toStartOfMinute(timestamp)"
	case hours <= 24:
		return "toStartOfFiveMinutes(timestamp)"
	case hours <= 168:
		return "toStartOfHour(timestamp)"
	default:
		return "toStartOfDay(timestamp)"
	}
}

// FormattedTimeBucketExpression returns the formatted time bucket expression
func FormattedTimeBucketExpression(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "formatDateTime(toStartOfMinute(timestamp), '" + TimeFormatISO8601 + "')"
	case hours <= 24:
		return "formatDateTime(toStartOfFiveMinutes(timestamp), '" + TimeFormatISO8601 + "')"
	case hours <= 168:
		return "formatDateTime(toStartOfHour(timestamp), '" + TimeFormatISO8601 + "')"
	default:
		return "formatDateTime(toStartOfDay(timestamp), '" + TimeFormatISO8601 + "')"
	}
}

// TimeBucketSeconds returns the bucket size in seconds
func TimeBucketSeconds(startMs, endMs int64) float64 {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return BucketOneMinute
	case hours <= 24:
		return BucketFiveMinutes
	case hours <= 168:
		return BucketOneHour
	default:
		return BucketOneDay
	}
}

// MetricSetToInClause converts a metric set to a SQL IN clause
// Example: MetricSetToInClause(KafkaConsumerLagMetrics) returns "'metric1', 'metric2', 'metric3'"
func MetricSetToInClause(metrics []string) string {
	if len(metrics) == 0 {
		return ""
	}
	var builder strings.Builder
	for i, metric := range metrics {
		if i > 0 {
			builder.WriteString("', '")
		} else {
			builder.WriteString("'")
		}
		builder.WriteString(metric)
	}
	builder.WriteString("'")
	return builder.String()
}
