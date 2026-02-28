package service

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/modules/log/model"
	"github.com/observability/observability-backend-go/modules/log/store"
)

// Service defines the business logic layer for logs.
type Service interface {
	GetLogs(ctx context.Context, teamUUID string, startMs, endMs int64, limit int, direction string, cursor model.LogCursor, filters model.LogFilters) (model.LogSearchResponse, error)
	GetLogHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogHistogramData, error)
	GetLogVolume(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogVolumeData, error)
	GetLogStats(ctx context.Context, teamUUID string, startMs, endMs int64, filters model.LogFilters) (model.LogStats, error)
	GetLogFields(ctx context.Context, teamUUID string, startMs, endMs int64, field string, filters model.LogFilters) ([]model.Facet, error)
	GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (model.LogSurroundingResponse, error)
	GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, timestamp int64, window int) (model.LogDetailResponse, error)
	GetTraceLogs(ctx context.Context, teamUUID, traceID string) (model.TraceLogsResponse, error)
}

type LogService struct {
	repo store.Repository
}

func NewService(repo store.Repository) *LogService {
	return &LogService{repo: repo}
}

func (s *LogService) GetLogs(ctx context.Context, teamUUID string, startMs, endMs int64, limit int, direction string, cursor model.LogCursor, filters model.LogFilters) (model.LogSearchResponse, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	result, err := s.repo.GetLogs(ctx, filters, limit, direction, cursor)
	if err != nil {
		return model.LogSearchResponse{}, err
	}

	currentOffset := cursor.Offset
	if currentOffset < 0 {
		currentOffset = 0
	}
	nextOffset := currentOffset + len(result.Logs)
	hasMore := int64(nextOffset) < result.Total

	var nextCursor string
	if hasMore {
		nextCursor = model.LogCursor{Offset: nextOffset}.Encode()
	}

	return model.LogSearchResponse{
		Logs:       result.Logs,
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Limit:      limit,
		Total:      result.Total,
		Facets: map[string][]model.Facet{
			"levels":   result.LevelFacets,
			"services": result.ServiceFacets,
			"hosts":    result.HostFacets,
		},
	}, nil
}

func (s *LogService) GetLogHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogHistogramData, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	if step == "" {
		step = s.autoStep(startMs, endMs)
	}

	buckets, err := s.repo.GetLogHistogram(ctx, filters, step)
	if err != nil {
		return model.LogHistogramData{}, err
	}

	return model.LogHistogramData{
		Buckets: buckets,
		Step:    step,
	}, nil
}

func (s *LogService) GetLogVolume(ctx context.Context, teamUUID string, startMs, endMs int64, step string, filters model.LogFilters) (model.LogVolumeData, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	if step == "" {
		step = s.autoStep(startMs, endMs)
	}

	buckets, err := s.repo.GetLogVolume(ctx, filters, step)
	if err != nil {
		return model.LogVolumeData{}, err
	}

	buckets = s.fillVolumeBuckets(buckets, startMs, endMs, step)

	return model.LogVolumeData{
		Buckets: buckets,
		Step:    step,
	}, nil
}

func (s *LogService) GetLogStats(ctx context.Context, teamUUID string, startMs, endMs int64, filters model.LogFilters) (model.LogStats, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	result, err := s.repo.GetLogStats(ctx, filters)
	if err != nil {
		return model.LogStats{}, err
	}

	return model.LogStats{
		Total: result.Total,
		Fields: map[string][]model.Facet{
			"level":        result.LevelFacets,
			"service_name": result.ServiceFacets,
			"host":         result.HostFacets,
			"pod":          result.PodFacets,
			"logger":       result.LoggerFacets,
		},
	}, nil
}

func (s *LogService) GetLogFields(ctx context.Context, teamUUID string, startMs, endMs int64, field string, filters model.LogFilters) ([]model.Facet, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	allowed := map[string]string{
		"level":        "level",
		"service_name": "service_name",
		"host":         "host",
		"pod":          "pod",
		"container":    "container",
		"logger":       "logger",
	}
	col, ok := allowed[field]
	if !ok {
		return nil, nil
	}

	return s.repo.GetLogFields(ctx, filters, col)
}

func (s *LogService) GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (model.LogSurroundingResponse, error) {
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	anchor, beforeRows, afterRows, err := s.repo.GetLogSurrounding(ctx, teamUUID, logID, before, after)
	if err != nil {
		return model.LogSurroundingResponse{}, err
	}

	return model.LogSurroundingResponse{
		Anchor: anchor,
		Before: beforeRows,
		After:  afterRows,
	}, nil
}

func (s *LogService) GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, timestamp int64, window int) (model.LogDetailResponse, error) {
	center := time.UnixMilli(timestamp).UTC()
	from := center.Add(-time.Duration(window) * time.Second)
	to := center.Add(time.Duration(window) * time.Second)

	log, contextLogs, err := s.repo.GetLogDetail(ctx, teamUUID, traceID, spanID, center, from, to)
	if err != nil {
		return model.LogDetailResponse{}, err
	}

	return model.LogDetailResponse{
		Log:         log,
		ContextLogs: contextLogs,
	}, nil
}

func (s *LogService) GetTraceLogs(ctx context.Context, teamUUID, traceID string) (model.TraceLogsResponse, error) {
	return s.repo.GetTraceLogs(ctx, teamUUID, traceID)
}

// autoStep picks a bucket size so the time window produces ~20-30 bars.
func (s *LogService) autoStep(startMs, endMs int64) string {
	ms := endMs - startMs
	targetBars := int64(30)
	stepMs := ms / targetBars

	switch {
	case stepMs <= 1*60*1000:
		return "1m"
	case stepMs <= 2*60*1000:
		return "2m"
	case stepMs <= 5*60*1000:
		return "5m"
	case stepMs <= 10*60*1000:
		return "10m"
	case stepMs <= 15*60*1000:
		return "15m"
	case stepMs <= 30*60*1000:
		return "30m"
	case stepMs <= 60*60*1000:
		return "1h"
	case stepMs <= 2*60*60*1000:
		return "2h"
	case stepMs <= 6*60*60*1000:
		return "6h"
	default:
		return "12h"
	}
}

// stepDuration returns the duration for a given step string.
func (s *LogService) stepDuration(step string) time.Duration {
	switch step {
	case "1m":
		return time.Minute
	case "2m":
		return 2 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "10m":
		return 10 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	default:
		return time.Minute
	}
}

// bucketKey truncates t to the step boundary and returns the bucket label string.
func (s *LogService) bucketKey(t time.Time, step string) string {
	d := s.stepDuration(step)
	truncated := t.Truncate(d)
	switch step {
	case "1h", "2h", "6h", "12h":
		return truncated.UTC().Format("2006-01-02 15:00:00")
	default:
		return truncated.UTC().Format("2006-01-02 15:04:00")
	}
}

// fillVolumeBuckets ensures every interval in [startMs, endMs] has a bucket,
// inserting zero-count entries for missing ones.
func (s *LogService) fillVolumeBuckets(buckets []model.LogVolumeBucket, startMs, endMs int64, step string) []model.LogVolumeBucket {
	d := s.stepDuration(step)
	start := time.UnixMilli(startMs).UTC().Truncate(d)
	end := time.UnixMilli(endMs).UTC()

	// Build lookup from existing data.
	byKey := make(map[string]model.LogVolumeBucket, len(buckets))
	for _, b := range buckets {
		byKey[b.TimeBucket] = b
	}

	var result []model.LogVolumeBucket
	for t := start; !t.After(end); t = t.Add(d) {
		key := s.bucketKey(t, step)
		if b, ok := byKey[key]; ok {
			result = append(result, b)
		} else {
			result = append(result, model.LogVolumeBucket{TimeBucket: key})
		}
	}
	return result
}
