package apm

import "fmt"

// OpenTelemetry Semantic Conventions for APM / Application Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/rpc/ & /system/process/

const (
	// Metric Names
	MetricRPCServerDuration        = "rpc.server.duration"
	MetricMessagingPublishDuration = "messaging.client.operation.duration"
	MetricProcessCPUTime           = "process.cpu.time"
	MetricProcessMemoryUsage       = "process.memory.usage"
	MetricProcessMemoryVirtual     = "process.memory.virtual"
	MetricProcessOpenFDs           = "process.open_file_descriptor.count"
	MetricProcessUptime            = "process.uptime"

	// Attribute Names
	AttrRPCSystem      = "rpc.system"
	AttrRPCService     = "rpc.service"
	AttrRPCMethod      = "rpc.method"
	AttrRPCGRPCStatus  = "rpc.grpc.status_code"
	AttrProcessCPUState = "process.cpu.state"
	AttrMessagingOp    = "messaging.operation.name"

	// Table Name
	TableMetrics = "metrics"

	// Column Names
	ColMetricName = "metric_name"
	ColTeamID     = "team_id"
	ColTimestamp  = "timestamp"
	ColValue      = "value"
)

// attrString returns a CH 26+ native JSON path expression for a String attribute.
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
