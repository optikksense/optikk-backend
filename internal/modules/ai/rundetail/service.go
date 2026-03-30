package rundetail

import (
	"context"
	"encoding/json"
	"strings"
)

type Service interface {
	GetRunDetail(teamID int64, spanID string) (*LLMRunDetail, error)
	GetRunMessages(teamID int64, spanID string) ([]LLMMessage, error)
	GetRunContext(teamID int64, spanID, traceID string) (*LLMRunContext, error)
}

type RunDetailService struct {
	repo Repository
}

func NewService(repo Repository) *RunDetailService {
	return &RunDetailService{repo: repo}
}

func (s *RunDetailService) GetRunDetail(teamID int64, spanID string) (*LLMRunDetail, error) {
	row, err := s.repo.GetRunDetail(context.Background(), teamID, spanID)
	if err != nil || row == nil {
		return nil, err
	}
	return row.toModel(), nil
}

func (s *RunDetailService) GetRunMessages(teamID int64, spanID string) ([]LLMMessage, error) {
	rows, err := s.repo.GetRunEvents(context.Background(), teamID, spanID)
	if err != nil {
		return nil, err
	}
	return extractMessages(rows), nil
}

func (s *RunDetailService) GetRunContext(teamID int64, spanID, traceID string) (*LLMRunContext, error) {
	rows, err := s.repo.GetTraceSpans(context.Background(), teamID, traceID)
	if err != nil {
		return nil, err
	}
	return buildContext(spanID, rows), nil
}

// extractMessages parses span events to extract prompt/completion messages.
func extractMessages(rows []runEventDTO) []LLMMessage {
	var messages []LLMMessage
	for _, row := range rows {
		raw := strings.TrimSpace(row.EventJSON)
		if raw == "" {
			continue
		}

		var ev struct {
			Name       string            `json:"name"`
			Attributes map[string]string `json:"attributes"`
		}
		if raw[0] == '{' {
			if err := json.Unmarshal([]byte(raw), &ev); err != nil {
				continue
			}
		} else {
			ev.Name = raw
		}

		switch ev.Name {
		case "gen_ai.content.prompt":
			messages = append(messages, parseGenAIContent(ev.Attributes, "gen_ai.prompt", "prompt")...)
		case "gen_ai.content.completion":
			messages = append(messages, parseGenAIContent(ev.Attributes, "gen_ai.completion", "completion")...)
		}
	}
	return messages
}

// parseGenAIContent extracts messages from a gen_ai event attribute.
// The attribute value is a JSON array of {role, content} objects.
func parseGenAIContent(attrs map[string]string, attrKey, msgType string) []LLMMessage {
	val, ok := attrs[attrKey]
	if !ok || val == "" {
		return nil
	}

	// Try parsing as JSON array of {role, content}
	var items []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(val), &items); err == nil {
		msgs := make([]LLMMessage, 0, len(items))
		for _, item := range items {
			msgs = append(msgs, LLMMessage{
				Role:    item.Role,
				Content: item.Content,
				Type:    msgType,
			})
		}
		return msgs
	}

	// Fallback: treat the whole value as a single message
	return []LLMMessage{{
		Role:    msgType,
		Content: val,
		Type:    msgType,
	}}
}

// buildContext constructs the ancestor chain, current span, and children.
func buildContext(spanID string, rows []traceContextSpanDTO) *LLMRunContext {
	spanMap := make(map[string]ChainSpan, len(rows))
	childrenOf := make(map[string][]string, len(rows))

	for _, row := range rows {
		sid := row.SpanID
		pid := row.ParentSpanID
		model := row.Model
		cs := ChainSpan{
			SpanID:        sid,
			ParentSpanID:  pid,
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			StartTime:     row.Timestamp,
			DurationMs:    row.DurationMs,
			HasError:      row.HasError,
			SpanKind:      row.KindString,
			Role:          classifyRole(row.OperationName, model),
			Model:         model,
		}
		spanMap[sid] = cs
		if pid != "" && pid != "0000000000000000" {
			childrenOf[pid] = append(childrenOf[pid], sid)
		}
	}

	ctx := &LLMRunContext{}

	current, ok := spanMap[spanID]
	if !ok {
		return ctx
	}
	ctx.Current = current

	// Walk ancestors
	visited := map[string]bool{spanID: true}
	pid := current.ParentSpanID
	for pid != "" && pid != "0000000000000000" && !visited[pid] {
		visited[pid] = true
		if ancestor, exists := spanMap[pid]; exists {
			ctx.Ancestors = append([]ChainSpan{ancestor}, ctx.Ancestors...)
			pid = ancestor.ParentSpanID
		} else {
			break
		}
	}

	// Direct children
	for _, cid := range childrenOf[spanID] {
		if child, exists := spanMap[cid]; exists {
			ctx.Children = append(ctx.Children, child)
		}
	}

	return ctx
}

// classifyRole determines the semantic role of a span.
func classifyRole(name, model string) string {
	if model != "" {
		return "llm_call"
	}
	lower := strings.ToLower(name)
	if strings.Contains(lower, "tool") {
		return "tool_call"
	}
	if strings.Contains(lower, "retriev") {
		return "retriever"
	}
	if strings.Contains(lower, "chain") {
		return "chain"
	}
	if strings.Contains(lower, "agent") {
		return "agent"
	}
	return "other"
}
