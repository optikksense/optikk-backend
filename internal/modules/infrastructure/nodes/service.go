package nodes

import (
	"context"
	"log/slog"
)

type Service interface {
	GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error)
	GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

type NodeService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &NodeService{repo: repo}
}

func (s *NodeService) GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	return s.repo.GetInfrastructureNodes(ctx, teamID, startMs, endMs)
}

func (s *NodeService) GetInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error) {
	summary, err := s.repo.GetInfrastructureNodeSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		slog.ErrorContext(ctx, "nodes: GetInfrastructureNodeSummary failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return InfrastructureNodeSummary{}, err
	}
	return summary, nil
}

func (s *NodeService) GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	return s.repo.GetInfrastructureNodeServices(ctx, teamID, host, startMs, endMs)
}
