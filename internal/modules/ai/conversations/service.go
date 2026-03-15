package conversations

type Service interface {
	ListConversations(teamID, startMs, endMs int64, limit int) ([]Conversation, error)
	GetConversation(teamID int64, conversationID string, startMs, endMs int64) ([]ConversationTurn, error)
}

type ConversationService struct{ repo Repository }

func NewService(repo Repository) *ConversationService {
	return &ConversationService{repo: repo}
}

func (s *ConversationService) ListConversations(teamID, startMs, endMs int64, limit int) ([]Conversation, error) {
	return s.repo.ListConversations(teamID, startMs, endMs, limit)
}

func (s *ConversationService) GetConversation(teamID int64, conversationID string, startMs, endMs int64) ([]ConversationTurn, error) {
	return s.repo.GetConversation(teamID, conversationID, startMs, endMs)
}
