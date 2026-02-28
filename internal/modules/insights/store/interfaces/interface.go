package interfaces

import (
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// Repository encapsulates data access logic for insights.
type Repository interface {
	GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (model.SloSummary, []model.SloBucket, error)
	GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]model.LogStreamItem, int64, []model.LogVolumeBucket, []model.Facet, []model.Facet, error)
}
