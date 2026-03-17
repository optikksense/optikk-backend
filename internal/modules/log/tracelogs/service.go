package tracelogs

import (
	"context"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetTraceLogs(ctx context.Context, teamID int64, traceID string) (TraceLogsResponse, error) {
	meta, metaErr := s.repo.GetTraceMeta(ctx, teamID, traceID)

	if meta != nil {
		traceStartRaw := dbutil.TimeFromAny(meta.TraceStart)
		traceEndRaw := dbutil.TimeFromAny(meta.TraceEnd)
		if !traceStartRaw.IsZero() && !traceEndRaw.IsZero() {
			startNs := uint64(traceStartRaw.Add(-2 * time.Second).UnixNano())
			endNs := uint64(traceEndRaw.Add(2 * time.Second).UnixNano())
			rows, err := s.repo.GetLogsByTraceWindow(ctx, teamID, traceID, startNs, endNs)
			if err == nil && len(rows) > 0 {
				return TraceLogsResponse{Logs: shared.MapLogRows(rows), IsSpeculative: false}, nil
			}
		}
	}

	rows, err := s.repo.GetLogsByTraceID(ctx, teamID, traceID)
	if err != nil {
		return TraceLogsResponse{}, err
	}
	if len(rows) > 0 {
		return TraceLogsResponse{Logs: shared.MapLogRows(rows), IsSpeculative: false}, nil
	}

	if metaErr != nil || meta == nil {
		return TraceLogsResponse{Logs: []shared.Log{}}, nil
	}

	traceStart := dbutil.TimeFromAny(meta.TraceStart)
	traceEnd := dbutil.TimeFromAny(meta.TraceEnd)
	serviceName := strings.TrimSpace(meta.ServiceName)
	httpMethod := strings.ToUpper(strings.TrimSpace(meta.HTTPMethod))
	route := shared.NormalizeRoute(meta.HTTPURL)
	if route == "" {
		route = shared.NormalizeRoute(meta.OperationName)
	}
	if traceStart.IsZero() || traceEnd.IsZero() || serviceName == "" {
		return TraceLogsResponse{Logs: []shared.Log{}}, nil
	}

	startNs := uint64(traceStart.Add(-2 * time.Second).UnixNano())
	endNs := uint64(traceEnd.Add(2 * time.Second).UnixNano())
	routeLike := "%"
	if route != "" {
		routeLike = "%" + route + "%"
	}

	fallbackRows, err := s.repo.GetFallbackLogs(ctx, teamID, serviceName, startNs, endNs, httpMethod, route, routeLike)
	if err != nil {
		return TraceLogsResponse{Logs: []shared.Log{}}, nil
	}

	return TraceLogsResponse{
		Logs:          shared.MapLogRows(fallbackRows),
		IsSpeculative: true,
	}, nil
}
