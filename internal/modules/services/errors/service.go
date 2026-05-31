package errors

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
)

// tsBucketTime converts a UInt32 ts_bucket (Unix-seconds, 5-min boundary)
// scanned natively from CH into the wire-model time.Time.
func tsBucketTime(b uint32) time.Time { return time.Unix(int64(b), 0).UTC() }

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// --- Service error rate ---

func (s *Service) GetServiceErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	var (
		raw []rawServiceRateRow
		err error
	)
	if serviceName == "" {
		raw, err = s.repo.ServiceErrorRateRowsAll(ctx, teamID, startMs, endMs)
	} else {
		raw, err = s.repo.ServiceErrorRateRowsByService(ctx, teamID, startMs, endMs, serviceName)
	}
	if err != nil {
		return nil, err
	}
	points := make([]TimeSeriesPoint, len(raw))
	for i, row := range raw {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		points[i] = TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    tsBucketTime(row.TsBucket),
			RequestCount: total,
			ErrorCount:   errs,
			ErrorRate:    computeErrorRate(errs, total),
			AvgLatency:   computeAvgLatency(row.DurationMsSum, row.RequestCount),
		}
	}
	return points, nil
}

// --- Error volume ---

func (s *Service) GetErrorVolume(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	var (
		raw []rawServiceErrorRow
		err error
	)
	if serviceName == "" {
		raw, err = s.repo.ErrorVolumeRowsAll(ctx, teamID, startMs, endMs)
	} else {
		raw, err = s.repo.ErrorVolumeRowsByService(ctx, teamID, startMs, endMs, serviceName)
	}
	if err != nil {
		return nil, err
	}
	points := make([]TimeSeriesPoint, 0, len(raw))
	for _, row := range raw {
		if row.ErrorCount == 0 {
			continue
		}
		points = append(points, TimeSeriesPoint{
			ServiceName: row.ServiceName,
			Timestamp:   tsBucketTime(row.TsBucket),
			ErrorCount:  int64(row.ErrorCount), //nolint:gosec // domain-bounded
		})
	}
	return points, nil
}


// --- Error groups ---

func (s *Service) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursorIn ErrorGroupsCursor) (PaginatedErrorGroups, error) {
	raw, err := s.fetchErrorGroups(ctx, teamID, startMs, endMs, serviceName, limit+1, cursorIn)
	if err != nil {
		return PaginatedErrorGroups{}, err
	}
	hasMore := len(raw) > limit
	if hasMore {
		raw = raw[:limit]
	}
	results := make([]ErrorGroup, len(raw))
	for i, row := range raw {
		code := httpBucketToCode(row.HTTPStatusBucket)
		results[i] = ErrorGroup{
			GroupID:         row.GroupID,
			ServiceName:     row.ServiceName,
			OperationName:   row.OperationName,
			StatusMessage:   row.StatusMessage,
			HTTPStatusCode:  code,
			ErrorCount:      int64(row.ErrorCount), //nolint:gosec // domain-bounded
			LastOccurrence:  row.LastOccurrence,
			FirstOccurrence: row.FirstOccurrence,
			SampleTraceID:   row.SampleTraceID,
		}
	}
	var nextCursor string
	if hasMore && len(raw) > 0 {
		lastRow := raw[len(raw)-1]
		nextCursor = cursor.Encode(ErrorGroupsCursor{
			ErrorCount: lastRow.ErrorCount,
			GroupID:    lastRow.GroupID,
		})
	}
	return PaginatedErrorGroups{
		Results: results,
		PageInfo: PageInfo{
			HasMore:    hasMore,
			NextCursor: nextCursor,
			Limit:      limit,
		},
	}, nil
}

func (s *Service) fetchErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int, cursorIn ErrorGroupsCursor) ([]rawErrorGroupRow, error) {
	if serviceName == "" {
		return s.repo.ErrorGroupRowsAll(ctx, teamID, startMs, endMs, limit, cursorIn)
	}
	return s.repo.ErrorGroupRowsByService(ctx, teamID, startMs, endMs, serviceName, limit, cursorIn)
}

func (s *Service) GetErrorGroupDetail(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*ErrorGroupDetail, error) {
	row, err := s.repo.ErrorGroupDetailRow(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return &ErrorGroupDetail{
		GroupID:          groupID,
		ServiceName:      row.ServiceName,
		OperationName:    row.OperationName,
		StatusMessage:    row.StatusMessage,
		HTTPStatusCode:   int(row.HTTPStatusCode),
		ErrorCount:       int64(row.ErrorCount), //nolint:gosec // domain-bounded
		LastOccurrence:   row.LastOccurrence,
		FirstOccurrence:  row.FirstOccurrence,
		SampleTraceID:    row.SampleTraceID,
		ExceptionType:    row.ExceptionType,
		SampleStacktrace: row.StackTrace,
	}, nil
}

func (s *Service) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]ErrorGroupTrace, error) {
	raw, err := s.repo.ErrorGroupTraceRows(ctx, teamID, startMs, endMs, groupID, limit)
	if err != nil {
		return nil, err
	}
	traces := make([]ErrorGroupTrace, len(raw))
	for i, row := range raw {
		traces[i] = ErrorGroupTrace{
			TraceID:    row.TraceID,
			SpanID:     row.SpanID,
			Timestamp:  row.Timestamp,
			DurationMs: row.DurationMs,
			StatusCode: row.StatusCode,
		}
	}
	return traces, nil
}

func (s *Service) GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]TimeSeriesPoint, error) {
	raw, err := s.repo.ErrorGroupTimeseriesRows(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	points := make([]TimeSeriesPoint, len(raw))
	for i, row := range raw {
		points[i] = TimeSeriesPoint{
			Timestamp:  tsBucketTime(row.TsBucket),
			ErrorCount: int64(row.Count), //nolint:gosec // domain-bounded
		}
	}
	return points, nil
}


// --- Error hotspot ---

func (s *Service) GetErrorHotspot(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	raw, err := s.repo.ErrorHotspotRows(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	cells := make([]ErrorHotspotCell, len(raw))
	for i, row := range raw {
		total := int64(row.TotalCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)  //nolint:gosec // domain-bounded
		cells[i] = ErrorHotspotCell{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ErrorRate:     computeErrorRate(errs, total),
			ErrorCount:    errs,
			TotalCount:    total,
		}
	}
	return cells, nil
}



// --- helpers (service-layer derivations) ---

func httpBucketToCode(bucket string) int {
	switch bucket {
	case "4xx":
		return 400
	case "5xx":
		return 500
	default:
		return 0
	}
}

func computeErrorRate(errs, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(errs) * 100.0 / float64(total)
}

func computeAvgLatency(sumMs float64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return sumMs / float64(count)
}
