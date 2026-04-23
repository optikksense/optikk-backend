package trace_paths //nolint:revive,stylecheck

import (
	"context"
	"log/slog"
)

type Service interface {
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]CriticalPathSpan, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]ErrorPathSpan, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service { return &service{repo: repo} }

func (s *service) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]CriticalPathSpan, error) {
	rows, err := s.repo.GetCriticalPath(ctx, teamID, traceID)
	if err != nil {
		slog.Error("trace_paths: GetCriticalPath failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildCriticalPath(rows), nil
}

func (s *service) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]ErrorPathSpan, error) {
	rows, err := s.repo.GetErrorPath(ctx, teamID, traceID)
	if err != nil {
		slog.Error("trace_paths: GetErrorPath failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildErrorPath(rows), nil
}

// buildCriticalPath runs the longest-path graph algorithm on the raw DB rows.
func buildCriticalPath(rows []criticalPathRow) []CriticalPathSpan {
	nodes, roots := indexNodes(rows)
	computeSubtreeEnds(nodes, roots)
	bestRoot := pickBestRoot(nodes, roots)
	return walkCriticalChain(nodes, bestRoot)
}

type criticalNode struct {
	row        criticalPathRow
	subtreeEnd int64
	children   []string
}

func indexNodes(rows []criticalPathRow) (map[string]*criticalNode, []string) {
	nodes := make(map[string]*criticalNode, len(rows))
	var roots []string
	for _, row := range rows {
		nodes[row.SpanID] = &criticalNode{row: row, subtreeEnd: row.EndNs}
		if isRootParentSpanID(row.ParentSpanID) {
			roots = append(roots, row.SpanID)
		}
	}
	for sid, n := range nodes {
		if !isRootParentSpanID(n.row.ParentSpanID) {
			if parent, ok := nodes[n.row.ParentSpanID]; ok {
				parent.children = append(parent.children, sid)
			}
		}
	}
	return nodes, roots
}

func computeSubtreeEnds(nodes map[string]*criticalNode, roots []string) {
	type frame struct {
		spanID   string
		childIdx int
	}
	for _, root := range roots {
		stack := []frame{{spanID: root}}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			n := nodes[top.spanID]
			if top.childIdx < len(n.children) {
				cid := n.children[top.childIdx]
				top.childIdx++
				stack = append(stack, frame{spanID: cid})
			} else {
				for _, cid := range n.children {
					if child := nodes[cid]; child.subtreeEnd > n.subtreeEnd {
						n.subtreeEnd = child.subtreeEnd
					}
				}
				stack = stack[:len(stack)-1]
			}
		}
	}
}

func pickBestRoot(nodes map[string]*criticalNode, roots []string) string {
	var bestRoot string
	var bestEnd int64
	for _, root := range roots {
		if n := nodes[root]; n.subtreeEnd > bestEnd {
			bestEnd = n.subtreeEnd
			bestRoot = root
		}
	}
	return bestRoot
}

func walkCriticalChain(nodes map[string]*criticalNode, root string) []CriticalPathSpan {
	var result []CriticalPathSpan
	cur := root
	for cur != "" {
		n, ok := nodes[cur]
		if !ok {
			break
		}
		result = append(result, CriticalPathSpan{
			SpanID:        n.row.SpanID,
			OperationName: n.row.OperationName,
			ServiceName:   n.row.ServiceName,
			DurationMs:    n.row.DurationMs,
		})
		if len(n.children) == 0 {
			break
		}
		cur = pickBestChild(nodes, n.children)
	}
	return result
}

func pickBestChild(nodes map[string]*criticalNode, children []string) string {
	var best string
	var bestEnd, bestStart int64
	for _, cid := range children {
		child := nodes[cid]
		if child.subtreeEnd > bestEnd || (child.subtreeEnd == bestEnd && child.row.StartNs > bestStart) {
			bestEnd = child.subtreeEnd
			bestStart = child.row.StartNs
			best = cid
		}
	}
	return best
}

// buildErrorPath builds the root→leaf error chain from raw error span rows.
func buildErrorPath(rows []errorPathRow) []ErrorPathSpan {
	spans := make(map[string]errorPathRow, len(rows))
	for _, r := range rows {
		spans[r.SpanID] = r
	}
	leafID := pickErrorLeaf(spans)
	if leafID == "" {
		return []ErrorPathSpan{}
	}
	chain := walkErrorChain(spans, leafID)
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

func pickErrorLeaf(spans map[string]errorPathRow) string {
	childOf := make(map[string]bool, len(spans))
	for _, s := range spans {
		if s.ParentSpanID != "" {
			childOf[s.ParentSpanID] = true
		}
	}
	for sid := range spans {
		if !childOf[sid] {
			return sid
		}
	}
	return ""
}

func walkErrorChain(spans map[string]errorPathRow, leafID string) []ErrorPathSpan {
	var chain []ErrorPathSpan
	cur := leafID
	for cur != "" {
		s, ok := spans[cur]
		if !ok {
			break
		}
		chain = append(chain, ErrorPathSpan{
			SpanID:        s.SpanID,
			ParentSpanID:  s.ParentSpanID,
			OperationName: s.OperationName,
			ServiceName:   s.ServiceName,
			Status:        s.Status,
			StatusMessage: s.StatusMessage,
			StartTime:     s.StartTime,
			DurationMs:    s.DurationMs,
		})
		cur = s.ParentSpanID
	}
	return chain
}
