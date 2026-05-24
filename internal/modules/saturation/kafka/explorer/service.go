package explorer

import (
	"context"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// Topic Domains
func (s *Service) GetTopicThroughput(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicThroughputRow, error) {
	return s.repo.QueryTopicThroughput(ctx, teamID, startMs, endMs, topic)
}

func (s *Service) GetTopicLag(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicLagRow, error) {
	return s.repo.QueryTopicLag(ctx, teamID, startMs, endMs, topic)
}

func (s *Service) GetTopicConsumers(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicConsumersRow, error) {
	return s.repo.QueryTopicConsumers(ctx, teamID, startMs, endMs, topic)
}

// Group Domains
func (s *Service) GetGroupPartitions(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupPartitionsRow, error) {
	return s.repo.QueryGroupPartitions(ctx, teamID, startMs, endMs, group)
}

func (s *Service) GetGroupCommits(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupCommitsRow, error) {
	return s.repo.QueryGroupCommits(ctx, teamID, startMs, endMs, group)
}

func (s *Service) GetGroupFetches(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupFetchesRow, error) {
	return s.repo.QueryGroupFetches(ctx, teamID, startMs, endMs, group)
}

func (s *Service) GetGroupHealth(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupHealthRow, error) {
	return s.repo.QueryGroupHealth(ctx, teamID, startMs, endMs, group)
}

// Detail Intersections
func (s *Service) GetTopicGroupThroughput(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicGroupThroughputRow, error) {
	return s.repo.QueryTopicGroupThroughput(ctx, teamID, startMs, endMs, topic)
}

func (s *Service) GetTopicGroupLag(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicGroupLagRow, error) {
	return s.repo.QueryTopicGroupLag(ctx, teamID, startMs, endMs, topic)
}

func (s *Service) GetGroupTopics(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupTopicRow, error) {
	return s.repo.QueryGroupTopics(ctx, teamID, startMs, endMs, group)
}
