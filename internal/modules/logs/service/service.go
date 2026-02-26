package service

import (
	"context"
	"time"

	"github.com/observability/observability-backend-go/internal/modules/logs/model"
	"github.com/observability/observability-backend-go/internal/modules/logs/store"
)

type LogService struct {
	repo store.Repository
}

func NewService(repo store.Repository) *LogService {
	return &LogService{repo: repo}
}

func (s *LogService) GetLogs(ctx context.Context, teamUUID string, startMs, endMs int64, limit int, direction string, cursor int64, filters model.LogFilters) (model.LogSearchResponse, error) {
	filters.TeamUUID = teamUUID
	filters.StartMs = startMs
	filters.EndMs = endMs

	logs, total, levelFacets, serviceFacets, hostFacets, err := s.repo.GetLogs(ctx, filters, limit, direction, cursor)
	if err != nil {
		return model.LogSearchResponse{}, err
	}

	var nextCursor int64
	if len(logs) == limit {
		nextCursor = logs[len(logs)-1].ID
	}

	return model.LogSearchResponse{
		Logs:       logs,
		HasMore:    len(logs) == limit,
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
	default: // "1m"
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	}
}

func (s *LogService) autoStep(startMs, endMs int64) string {
	ms := endMs - startMs
	switch {
	case ms <= 30*60*1000:
		return "1m"
	case ms <= 3*60*60*1000:
		return "5m"
	case ms <= 24*60*60*1000:
		return "30m"
	default:
		return "1h"
	}
}
