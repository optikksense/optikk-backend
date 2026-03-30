package conversations

type conversationRowDTO Conversation
type conversationTurnRowDTO ConversationTurn

func mapConversationDTOs(rows []conversationRowDTO) []Conversation {
	out := make([]Conversation, len(rows))
	for i, row := range rows {
		out[i] = Conversation(row)
	}
	return out
}

func mapConversationTurnDTOs(rows []conversationTurnRowDTO) []ConversationTurn {
	out := make([]ConversationTurn, len(rows))
	for i, row := range rows {
		out[i] = ConversationTurn(row)
	}
	return out
}
