package interfaces

import "github.com/observability/observability-backend-go/internal/modules/saturation/model"

// Repository encapsulates data access logic for saturation metrics.
type Repository interface {
	GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]model.SaturationMetric, error)
	GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]model.SaturationTimeSeries, error)
}
