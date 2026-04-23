package errors

import (
	"context"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) ErrorGroups(ctx context.Context, teamID int64, req ErrorGroupsRequest) (ErrorGroupsResponse, error) {
	rows, err := s.repo.ErrorGroups(ctx, teamID, req.StartMs, req.EndMs, req.ServiceName, req.Limit)
	if err != nil {
		return ErrorGroupsResponse{}, err
	}
	groups := make([]ErrorGroup, len(rows))
	for i, r := range rows {
		groups[i] = ErrorGroup{
			ExceptionType: r.ExceptionType,
			StatusMessage: r.StatusMessage,
			Service:       r.Service,
			Count:         r.Count,
		}
	}
	return ErrorGroupsResponse{Groups: groups}, nil
}

func (s *Service) Timeseries(ctx context.Context, teamID int64, req TimeseriesRequest) (TimeseriesResponse, error) {
	rows, err := s.repo.Timeseries(ctx, teamID, req.StartMs, req.EndMs, req.ServiceName)
	if err != nil {
		return TimeseriesResponse{}, err
	}
	out := make([]TimeseriesBucket, len(rows))
	for i, r := range rows {
		out[i] = TimeseriesBucket{TimeBucket: r.TimeBucket, Errors: r.Errors, Total: r.Total}
	}
	return TimeseriesResponse{Buckets: out}, nil
}
