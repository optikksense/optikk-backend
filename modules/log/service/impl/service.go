package impl

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/modules/log/model"
	"github.com/observability/observability-backend-go/modules/log/store"
)

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

	logs, total, levelFacets, serviceFacets, hostFacets, err := s.repo.GetLogs(ctx, filters, limit, direction, cursor)
	if err != nil {
		return model.LogSearchResponse{}, err
	}

	currentOffset := cursor.Offset
	if currentOffset < 0 {
		currentOffset = 0
	}
	nextOffset := currentOffset + len(logs)
	hasMore := int64(nextOffset) < total

	var nextCursor string
	if hasMore {
		nextCursor = model.LogCursor{Offset: nextOffset}.Encode()
	}

	return model.LogSearchResponse{
		Logs:       logs,
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Limit:      limit,
		Total:      total,
		Facets: map[string][]model.Facet{
			"levels":   levelFacets,
			"services": serviceFacets,
			"hosts":    hostFacets,
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
	bucket := s.logBucketExpr(step)

	buckets, err := s.repo.GetLogHistogram(ctx, filters, bucket)
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
	bucket := s.logBucketExpr(step)

	buckets, err := s.repo.GetLogVolume(ctx, filters, bucket)
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

	total, levelFacets, serviceFacets, hostFacets, podFacets, loggerFacets, err := s.repo.GetLogStats(ctx, filters)
	if err != nil {
		return model.LogStats{}, err
	}

	return model.LogStats{
		Total: total,
		Fields: map[string][]model.Facet{
			"level":        levelFacets,
			"service_name": serviceFacets,
			"host":         hostFacets,
			"pod":          podFacets,
			"logger":       loggerFacets,
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
		return nil, nil // Should be handled by handler validation
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

func (s *LogService) GetTraceLogs(ctx context.Context, teamUUID, traceID string) ([]model.Log, error) {
	return s.repo.GetTraceLogs(ctx, teamUUID, traceID)
}

func (s *LogService) logBucketExpr(step string) string {
	switch step {
	case "1m":
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	case "2m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 2 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "5m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 5 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "10m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 10 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "15m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 15 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "30m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 30 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "1h":
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00')`
	case "2h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 2 HOUR), '%Y-%m-%d %H:00:00')`
	case "6h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 6 HOUR), '%Y-%m-%d %H:00:00')`
	case "12h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 12 HOUR), '%Y-%m-%d %H:00:00')`
	default:
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	}
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
