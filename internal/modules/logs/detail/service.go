package detail

import (
	"context"
	"time"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetLogSurrounding(ctx context.Context, teamID int64, logID string, before, after int) (LogSurroundingResponse, error) {
	anchor, err := s.repo.GetLogByID(ctx, teamID, logID)
	if err != nil {
		return LogSurroundingResponse{}, err
	}

	tsLow := anchor.Timestamp.Add(-time.Hour)
	tsHigh := anchor.Timestamp.Add(time.Hour)

	beforeRows, _ := s.repo.GetSurroundingBefore(ctx, teamID, anchor.ServiceName, tsLow, tsHigh, anchor.Timestamp, logID, before)
	afterRows, _ := s.repo.GetSurroundingAfter(ctx, teamID, anchor.ServiceName, tsLow, tsHigh, anchor.Timestamp, logID, after)

	for i, j := 0, len(beforeRows)-1; i < j; i, j = i+1, j-1 {
		beforeRows[i], beforeRows[j] = beforeRows[j], beforeRows[i]
	}

	return LogSurroundingResponse{
		Anchor: anchor.ToLog(),
		Before: shared.MapLogRows(beforeRows),
		After:  shared.MapLogRows(afterRows),
	}, nil
}

func (s *Service) GetLogDetail(ctx context.Context, teamID int64, traceID, spanID string, centerNs, fromNs, toNs uint64) (LogDetailResponse, error) {
	from := time.Unix(0, int64(centerNs-1_000_000_000))
	to := time.Unix(0, int64(centerNs+1_000_000_000))
	logRow, err := s.repo.GetLogByTraceSpanWindow(ctx, teamID, traceID, spanID, from, to)
	if err != nil {
		return LogDetailResponse{}, err
	}

	contextRows := []shared.LogRowDTO{}
	if logRow.ServiceName != "" {
		cFrom := time.Unix(0, int64(fromNs))
		cTo := time.Unix(0, int64(toNs))
		contextRows, _ = s.repo.GetContextLogs(ctx, teamID, logRow.ServiceName, cFrom, cTo)
	}

	return LogDetailResponse{
		Log:         logRow.ToLog(),
		ContextLogs: shared.MapLogRows(contextRows),
	}, nil
}
