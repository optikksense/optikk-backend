package interfaces

// Repository describes insights data-access operations.
type Repository interface {
	GetInsightResourceUtilization(teamUUID string, startMs, endMs int64) ([]map[string]any, []map[string]any, []map[string]any, []map[string]any, error)
	GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (map[string]any, []map[string]any, error)
	GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]map[string]any, int64, []map[string]any, []map[string]any, []map[string]any, error)
	GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (map[string]any, []map[string]any, []map[string]any, error)
	GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (map[string]any, []map[string]any, []map[string]any, error)
}
