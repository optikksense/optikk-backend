package detail

import (
	"context"

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

	const oneHourNs = uint64(3_600_000_000_000)
	tsLow := anchor.Timestamp - oneHourNs
	tsHigh := anchor.Timestamp + oneHourNs

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
	logRow, err := s.repo.GetLogByTraceSpanWindow(ctx, teamID, traceID, spanID, centerNs-1_000_000_000, centerNs+1_000_000_000)
	if err != nil {
		return LogDetailResponse{}, err
	}

	contextRows := []shared.LogRowDTO{}
	if logRow.ServiceName != "" {
		contextRows, _ = s.repo.GetContextLogs(ctx, teamID, logRow.ServiceName, fromNs, toNs)
	}

	return LogDetailResponse{
		Log:         logRow.ToLog(),
		ContextLogs: shared.MapLogRows(contextRows),
	}, nil
}
