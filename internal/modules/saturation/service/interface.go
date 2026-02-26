package service

import "github.com/observability/observability-backend-go/internal/modules/saturation/model"

// Service encapsulates the business logic for the saturation module.
type Service interface {
	GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]model.SaturationMetric, error)
	GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]model.SaturationTimeSeries, error)
}
