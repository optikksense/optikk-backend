package interfaces

import (
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// Repository encapsulates data access logic for insights.
type Repository interface {
	GetInsightResourceUtilization(teamUUID string, startMs, endMs int64) ([]model.ServiceResource, []model.InstanceResource, []model.InfraResource, []model.ResourceBucket, error)
	GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (model.SloSummary, []model.SloBucket, error)
	GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]model.LogStreamItem, int64, []model.LogVolumeBucket, []model.Facet, []model.Facet, error)
	GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (model.DbCacheSummary, []model.DbTableMetric, []model.DbSystemBreakdown, error)
	GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (model.MqSummary, []model.MqBucket, []model.MqTopQueue, error)
}
