package conversations

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) ListConversations(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]Conversation, error) {
	rows, err := s.repo.ListConversations(ctx, teamID, startMs, endMs, limit)
	if err != nil {
		return nil, err
	}
	return mapConversationDTOs(rows), nil
}

func (s *Service) GetConversation(ctx context.Context, teamID int64, conversationID string, startMs, endMs int64) ([]ConversationTurn, error) {
	rows, err := s.repo.GetConversation(ctx, teamID, conversationID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapConversationTurnDTOs(rows), nil
}
