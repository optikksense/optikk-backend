package store

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/internal/modules/logs/model"
)

// Repository defines the data access layer for logs.
type Repository interface {
	GetLogs(ctx context.Context, f model.LogFilters, limit int, direction string, cursor model.LogCursor) (logs []model.Log, total int64, levelFacets, serviceFacets, hostFacets []model.Facet, err error)
	GetLogHistogram(ctx context.Context, f model.LogFilters, bucketExpr string) ([]model.LogHistogramBucket, error)
	GetLogVolume(ctx context.Context, f model.LogFilters, bucketExpr string) ([]model.LogVolumeBucket, error)
	GetLogStats(ctx context.Context, f model.LogFilters) (total int64, levelFacets, serviceFacets, hostFacets, podFacets, loggerFacets []model.Facet, err error)
	GetLogFields(ctx context.Context, f model.LogFilters, col string) ([]model.Facet, error)
	GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (anchor model.Log, beforeRows, afterRows []model.Log, err error)
	GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, center, from, to time.Time) (log model.Log, contextLogs []model.Log, err error)
	GetTraceLogs(ctx context.Context, teamUUID, traceID string) ([]model.Log, error)
}
