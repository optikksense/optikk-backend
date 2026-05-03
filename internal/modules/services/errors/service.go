package errors

import (
	"context"
	"fmt"
)

// GroupIdentity is the identity tuple an ErrorGroup hash resolves back to.
// Lives in the service layer because hash → identity resolution is service work.
type GroupIdentity struct {
	Service       string
	Operation     string
	StatusMessage string
	HTTPCode      int
}

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
			Timestamp:    row.Timestamp,
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
			Timestamp:   row.Timestamp,
			ErrorCount:  int64(row.ErrorCount), //nolint:gosec // domain-bounded
		})
	}
	return points, nil
}

// --- Latency during error windows ---
// Reuses the ServiceErrorRate query — same shape, post-processing drops zero-error rows.

func (s *Service) GetLatencyDuringErrorWindows(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
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
	points := make([]TimeSeriesPoint, 0, len(raw))
	for _, row := range raw {
		if row.ErrorCount == 0 {
			continue
		}
		points = append(points, TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: int64(row.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(row.ErrorCount),   //nolint:gosec // domain-bounded
			AvgLatency:   computeAvgLatency(row.DurationMsSum, row.RequestCount),
		})
	}
	return points, nil
}

// --- Error groups ---

func (s *Service) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	raw, err := s.fetchErrorGroups(ctx, teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		return nil, err
	}
	groups := make([]ErrorGroup, len(raw))
	for i, row := range raw {
		code := httpBucketToCode(row.HTTPStatusBucket)
		groups[i] = ErrorGroup{
			GroupID:         ErrorGroupID(row.ServiceName, row.OperationName, row.StatusMessage, code),
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
	return groups, nil
}

func (s *Service) fetchErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]rawErrorGroupRow, error) {
	if serviceName == "" {
		return s.repo.ErrorGroupRowsAll(ctx, teamID, startMs, endMs, limit)
	}
	return s.repo.ErrorGroupRowsByService(ctx, teamID, startMs, endMs, serviceName, limit)
}

// resolveGroupID re-aggregates the error-group list for the window and finds
// the row whose hash matches groupID. No cache: each drill-in pays for one
// fresh aggregation.
func (s *Service) resolveGroupID(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (GroupIdentity, error) {
	raw, err := s.repo.ErrorGroupRowsAll(ctx, teamID, startMs, endMs, 500)
	if err != nil {
		return GroupIdentity{}, err
	}
	for _, row := range raw {
		code := httpBucketToCode(row.HTTPStatusBucket)
		if ErrorGroupID(row.ServiceName, row.OperationName, row.StatusMessage, code) == groupID {
			return GroupIdentity{
				Service:       row.ServiceName,
				Operation:     row.OperationName,
				StatusMessage: row.StatusMessage,
				HTTPCode:      code,
			}, nil
		}
	}
	return GroupIdentity{}, fmt.Errorf("error group %s not found", groupID)
}

func (s *Service) GetErrorGroupDetail(ctx context.Context, teamID int64, startMs, endMs int64, groupID string) (*ErrorGroupDetail, error) {
	ident, err := s.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	row, err := s.repo.ErrorGroupDetailRow(ctx, teamID, startMs, endMs, ident)
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
		StatusMessage:   row.StatusMessage,
		HTTPStatusCode:  int(row.HTTPStatusCode),
		ErrorCount:      row.ErrorCount,
		LastOccurrence:  row.LastOccurrence,
		FirstOccurrence: row.FirstOccurrence,
		SampleTraceID:   row.SampleTraceID,
		ExceptionType:   row.ExceptionType,
		StackTrace:      row.StackTrace,
	}, nil
}

func (s *Service) GetErrorGroupTraces(ctx context.Context, teamID int64, startMs, endMs int64, groupID string, limit int) ([]ErrorGroupTrace, error) {
	ident, err := s.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	raw, err := s.repo.ErrorGroupTraceRows(ctx, teamID, startMs, endMs, ident, limit)
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
	ident, err := s.resolveGroupID(ctx, teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}
	raw, err := s.repo.ErrorGroupTimeseriesRows(ctx, teamID, startMs, endMs, ident)
	if err != nil {
		return nil, err
	}
	points := make([]TimeSeriesPoint, len(raw))
	for i, row := range raw {
		points[i] = TimeSeriesPoint{
			Timestamp:  row.Timestamp,
			ErrorCount: int64(row.Count), //nolint:gosec // domain-bounded
		}
	}
	return points, nil
}

// --- Exception rate by type ---

func (s *Service) GetExceptionRateByType(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	var (
		raw []rawExceptionRateRow
		err error
	)
	if serviceName == "" {
		raw, err = s.repo.ExceptionRateRowsAll(ctx, teamID, startMs, endMs)
	} else {
		raw, err = s.repo.ExceptionRateRowsByService(ctx, teamID, startMs, endMs, serviceName)
	}
	if err != nil {
		return nil, err
	}
	points := make([]ExceptionRatePoint, len(raw))
	for i, row := range raw {
		points[i] = ExceptionRatePoint{
			Timestamp:     row.Timestamp,
			ExceptionType: row.ExceptionType,
			Count:         int64(row.Count), //nolint:gosec // domain-bounded
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

// --- HTTP 5xx by route ---

func (s *Service) GetHTTP5xxByRoute(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	var (
		raw []rawHTTP5xxRow
		err error
	)
	if serviceName == "" {
		raw, err = s.repo.HTTP5xxByRouteRowsAll(ctx, teamID, startMs, endMs)
	} else {
		raw, err = s.repo.HTTP5xxByRouteRowsByService(ctx, teamID, startMs, endMs, serviceName)
	}
	if err != nil {
		return nil, err
	}
	out := make([]HTTP5xxByRoute, len(raw))
	for i, row := range raw {
		out[i] = HTTP5xxByRoute{
			HTTPRoute:   row.HTTPRoute,
			ServiceName: row.ServiceName,
			Count:       row.Count,
		}
	}
	return out, nil
}

// --- Fingerprint list ---

func (s *Service) ListFingerprints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorFingerprint, error) {
	var (
		raw []rawErrorFingerprintRow
		err error
	)
	if serviceName == "" {
		raw, err = s.repo.FingerprintRowsAll(ctx, teamID, startMs, endMs, limit)
	} else {
		raw, err = s.repo.FingerprintRowsByService(ctx, teamID, startMs, endMs, serviceName, limit)
	}
	if err != nil {
		return nil, err
	}
	out := make([]ErrorFingerprint, len(raw))
	for i, row := range raw {
		out[i] = ErrorFingerprint{
			Fingerprint:   row.Fingerprint,
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ExceptionType: row.ExceptionType,
			StatusMessage: row.StatusMessage,
			FirstSeen:     row.FirstSeen,
			LastSeen:      row.LastSeen,
			Count:         int64(row.Count), //nolint:gosec // domain-bounded
			SampleTraceID: row.SampleTraceID,
		}
	}
	return out, nil
}

// --- Fingerprint trend ---

func (s *Service) GetFingerprintTrend(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]FingerprintTrendPoint, error) {
	raw, err := s.repo.FingerprintTrendRows(ctx, teamID, startMs, endMs, serviceName, operationName, exceptionType, statusMessage)
	if err != nil {
		return nil, err
	}
	out := make([]FingerprintTrendPoint, len(raw))
	for i, row := range raw {
		out[i] = FingerprintTrendPoint{
			Timestamp: row.Timestamp,
			Count:     int64(row.Count), //nolint:gosec // domain-bounded
		}
	}
	return out, nil
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
