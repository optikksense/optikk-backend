package service

import (
	"context"

	"github.com/observability/observability-backend-go/internal/modules/logs/model"
)

// Service defines the business logic layer for logs.
type Service interface {
	GetLogs(ctx context.Context, teamUUID string, startMs, endMs int64, limit int, direction string, cursor int64, filters model.LogFilters) (model.LogSearchResponse, error)
	GetLogHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogHistogramData, error)
	GetLogVolume(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogVolumeData, error)
	GetLogStats(ctx context.Context, teamUUID string, startMs, endMs int64, filters model.LogFilters) (model.LogStats, error)
	GetLogFields(ctx context.Context, teamUUID string, startMs, endMs int64, field string, filters model.LogFilters) ([]model.Facet, error)
	GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (model.LogSurroundingResponse, error)
	GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, timestamp int64, window int) (model.LogDetailResponse, error)
	GetTraceLogs(ctx context.Context, teamUUID, traceID string) ([]model.Log, error)
}
