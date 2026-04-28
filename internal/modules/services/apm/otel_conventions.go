package apm

// OpenTelemetry semantic-convention metric names referenced by APM endpoints.
// Reference: https://opentelemetry.io/docs/specs/semconv/rpc/ & /system/process/

const (
	MetricRPCServerDuration        = "rpc.server.duration"
	MetricMessagingPublishDuration = "messaging.client.operation.duration"
	MetricProcessCPUTime           = "process.cpu.time"
	MetricProcessMemoryUsage       = "process.memory.usage"
	MetricProcessMemoryVirtual     = "process.memory.virtual"
	MetricProcessOpenFDs           = "process.open_file_descriptor.count"
	MetricProcessUptime            = "process.uptime"
)
