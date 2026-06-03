package errors

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

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

	grain := timebucket.DisplayGrain(endMs - startMs)
	startTime := time.UnixMilli(startMs).UTC().Truncate(grain)
	endTime := time.UnixMilli(endMs).UTC().Truncate(grain)

	if serviceName == "" {
		serviceNames := make(map[string]bool)
		for _, row := range raw {
			if row.ServiceName != "" {
				serviceNames[row.ServiceName] = true
			}
		}

		rowMap := make(map[string]map[int64]rawServiceRateRow)
		for svc := range serviceNames {
			rowMap[svc] = make(map[int64]rawServiceRateRow)
		}
		for _, row := range raw {
			ts := row.BucketAt.UTC().Truncate(grain).Unix()
			if rowMap[row.ServiceName] != nil {
				rowMap[row.ServiceName][ts] = row
			}
		}

		var points []TimeSeriesPoint
		for svc := range serviceNames {
			for t := startTime; !t.After(endTime); t = t.Add(grain) {
				row, ok := rowMap[svc][t.Unix()]
				var total, errs int64
				var durationMsSum float64
				if ok {
					total = int64(row.RequestCount)
					errs = int64(row.ErrorCount)
					durationMsSum = row.DurationMsSum
				}
				points = append(points, TimeSeriesPoint{
					ServiceName:  svc,
					Timestamp:    t,
					RequestCount: total,
					ErrorCount:   errs,
					ErrorRate:    computeErrorRate(errs, total),
					AvgLatency:   computeAvgLatency(durationMsSum, uint64(total)),
				})
			}
		}
		return points, nil
	}

	rowMap := make(map[int64]rawServiceRateRow)
	for _, row := range raw {
		ts := row.BucketAt.UTC().Truncate(grain).Unix()
		rowMap[ts] = row
	}

	var points []TimeSeriesPoint
	for t := startTime; !t.After(endTime); t = t.Add(grain) {
		row, ok := rowMap[t.Unix()]
		var total, errs int64
		var durationMsSum float64
		if ok {
			total = int64(row.RequestCount)
			errs = int64(row.ErrorCount)
			durationMsSum = row.DurationMsSum
		}
		points = append(points, TimeSeriesPoint{
			ServiceName:  serviceName,
			Timestamp:    t,
			RequestCount: total,
			ErrorCount:   errs,
			ErrorRate:    computeErrorRate(errs, total),
			AvgLatency:   computeAvgLatency(durationMsSum, uint64(total)),
		})
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

	grain := timebucket.DisplayGrain(endMs - startMs)
	startTime := time.UnixMilli(startMs).UTC().Truncate(grain)
	endTime := time.UnixMilli(endMs).UTC().Truncate(grain)

	if serviceName == "" {
		serviceNames := make(map[string]bool)
		for _, row := range raw {
			if row.ServiceName != "" {
				serviceNames[row.ServiceName] = true
			}
		}

		rowMap := make(map[string]map[int64]rawServiceErrorRow)
		for svc := range serviceNames {
			rowMap[svc] = make(map[int64]rawServiceErrorRow)
		}
		for _, row := range raw {
			ts := row.BucketAt.UTC().Truncate(grain).Unix()
			if rowMap[row.ServiceName] != nil {
				rowMap[row.ServiceName][ts] = row
			}
		}

		var points []TimeSeriesPoint
		for svc := range serviceNames {
			for t := startTime; !t.After(endTime); t = t.Add(grain) {
				row, ok := rowMap[svc][t.Unix()]
				var errs int64
				if ok {
					errs = int64(row.ErrorCount)
				}
				points = append(points, TimeSeriesPoint{
					ServiceName: svc,
					Timestamp:   t,
					ErrorCount:  errs,
				})
			}
		}
		return points, nil
	}

	rowMap := make(map[int64]rawServiceErrorRow)
	for _, row := range raw {
		ts := row.BucketAt.UTC().Truncate(grain).Unix()
		rowMap[ts] = row
	}

	var points []TimeSeriesPoint
	for t := startTime; !t.After(endTime); t = t.Add(grain) {
		row, ok := rowMap[t.Unix()]
		var errs int64
		if ok {
			errs = int64(row.ErrorCount)
		}
		points = append(points, TimeSeriesPoint{
			ServiceName: serviceName,
			Timestamp:   t,
			ErrorCount:  errs,
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
		GroupID:         groupID,
		ServiceName:     row.ServiceName,
		OperationName:   row.OperationName,
		HTTPStatusCode:  int(row.HTTPStatusCode),
		ErrorCount:      int64(row.ErrorCount), //nolint:gosec // domain-bounded
		LastOccurrence:  row.LastOccurrence,
		FirstOccurrence: row.FirstOccurrence,
		ExceptionType:   row.ExceptionType,
	}, nil
}

// facetColumns are the spans_1m dimensions exposed as "Where it happens" facets.
// This list is the SQL-injection whitelist for ErrorGroupFacetRows — only these
// column names are ever interpolated into the query.
var facetColumns = []string{"service_version", "environment", "pod", "http_route"}

func (s *Service) GetErrorGroupLatestOccurrence(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*ErrorLatestOccurrence, error) {
	row, err := s.repo.ErrorGroupLatestOccurrenceRow(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return &ErrorLatestOccurrence{
		TraceID:        row.TraceID,
		SpanID:         row.SpanID,
		Timestamp:      row.Timestamp,
		DurationMs:     row.DurationMs,
		Message:        row.ExceptionMessage,
		Stacktrace:     row.StackTrace,
		HTTPMethod:     row.HTTPMethod,
		HTTPRoute:      row.HTTPRoute,
		HTTPStatusCode: row.HTTPStatusCode,
		ServiceVersion: row.ServiceVersion,
		Environment:    row.Environment,
		Pod:            row.Pod,
		Host:           row.Host,
	}, nil
}

func (s *Service) GetErrorGroupFacets(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]ErrorFacetGroup, error) {
	groups := make([]ErrorFacetGroup, 0, len(facetColumns))
	for _, col := range facetColumns {
		raw, err := s.repo.ErrorGroupFacetRows(ctx, teamID, startMs, endMs, groupID, col)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		var total int64
		for _, r := range raw {
			total += int64(r.Count) //nolint:gosec // domain-bounded
		}
		facets := make([]ErrorFacet, len(raw))
		for i, r := range raw {
			count := int64(r.Count) //nolint:gosec // domain-bounded
			facets[i] = ErrorFacet{
				Name:  r.Value,
				Count: count,
				Pct:   facetPct(count, total),
			}
		}
		groups = append(groups, ErrorFacetGroup{Key: col, Facets: facets})
	}
	return groups, nil
}

func facetPct(count, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(count) * 100.0 / float64(total)
}

func (s *Service) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int, cursorIn ErrorTracesCursor) (PaginatedErrorTraces, error) {
	raw, err := s.repo.ErrorGroupTraceRows(ctx, teamID, startMs, endMs, groupID, limit+1, cursorIn)
	if err != nil {
		return PaginatedErrorTraces{}, err
	}
	hasMore := len(raw) > limit
	if hasMore {
		raw = raw[:limit]
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
	var nextCursor string
	if hasMore && len(raw) > 0 {
		lastRow := raw[len(raw)-1]
		nextCursor = cursor.Encode(ErrorTracesCursor{
			Timestamp: lastRow.Timestamp,
			SpanID:    lastRow.SpanID,
		})
	}
	return PaginatedErrorTraces{
		Results: traces,
		PageInfo: PageInfo{
			HasMore:    hasMore,
			NextCursor: nextCursor,
			Limit:      limit,
		},
	}, nil
}

func (s *Service) GetErrorGroupTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) ([]TimeSeriesPoint, error) {
	raw, err := s.repo.ErrorGroupTimeseriesRows(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	grain := timebucket.DisplayGrain(endMs - startMs)
	startTime := time.UnixMilli(startMs).UTC().Truncate(grain)
	endTime := time.UnixMilli(endMs).UTC().Truncate(grain)

	rowMap := make(map[int64]rawTimeBucketCountRow)
	for _, row := range raw {
		ts := row.BucketAt.UTC().Truncate(grain).Unix()
		rowMap[ts] = row
	}

	var points []TimeSeriesPoint
	for t := startTime; !t.After(endTime); t = t.Add(grain) {
		row, ok := rowMap[t.Unix()]
		var count int64
		if ok {
			count = int64(row.Count)
		}
		points = append(points, TimeSeriesPoint{
			Timestamp:  t,
			ErrorCount: count,
		})
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
			GroupID:       row.GroupID,
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
