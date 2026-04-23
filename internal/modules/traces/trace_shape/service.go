package trace_shape //nolint:revive,stylecheck

import (
	"context"
	"log/slog"
)

type Service interface {
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]SpanKindDuration, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]FlamegraphFrame, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service { return &service{repo: repo} }

func (s *service) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]SpanKindDuration, error) {
	rows, err := s.repo.GetSpanKindBreakdown(ctx, teamID, traceID)
	if err != nil {
		slog.Error("trace_shape: GetSpanKindBreakdown failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	var totalMs float64
	result := make([]SpanKindDuration, len(rows))
	for i, row := range rows {
		totalMs += row.TotalDuraMs
		result[i] = SpanKindDuration{SpanKind: row.SpanKind, TotalDuraMs: row.TotalDuraMs, SpanCount: row.SpanCount}
	}
	if totalMs > 0 {
		for i := range result {
			result[i].PctOfTrace = result[i].TotalDuraMs * 100.0 / totalMs
		}
	}
	return result, nil
}

func (s *service) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]FlamegraphFrame, error) {
	rows, err := s.repo.GetFlamegraphData(ctx, teamID, traceID)
	if err != nil {
		slog.Error("trace_shape: GetFlamegraphData failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildFlamegraph(rows), nil
}

// buildFlamegraph builds depth-first flamegraph frames from raw span rows.
func buildFlamegraph(rows []flamegraphRow) []FlamegraphFrame {
	nodes, roots := indexFlameNodes(rows)
	childSum := sumChildDurations(nodes)
	return walkFrames(nodes, roots, childSum)
}

type flameNode struct {
	row      flamegraphRow
	children []string
}

func indexFlameNodes(rows []flamegraphRow) (map[string]*flameNode, []string) {
	nodes := make(map[string]*flameNode, len(rows))
	var roots []string
	for i := range rows {
		row := &rows[i]
		nodes[row.SpanID] = &flameNode{row: *row}
		if row.ParentSpanID == "" {
			roots = append(roots, row.SpanID)
		}
	}
	for sid, n := range nodes {
		if n.row.ParentSpanID != "" {
			if p, ok := nodes[n.row.ParentSpanID]; ok {
				p.children = append(p.children, sid)
			}
		}
	}
	return nodes, roots
}

func sumChildDurations(nodes map[string]*flameNode) map[string]float64 {
	out := make(map[string]float64, len(nodes))
	for _, n := range nodes {
		if n.row.ParentSpanID != "" {
			out[n.row.ParentSpanID] += n.row.DurationMs
		}
	}
	return out
}

type flameFrame struct {
	spanID string
	level  int
}

func walkFrames(nodes map[string]*flameNode, roots []string, childSum map[string]float64) []FlamegraphFrame {
	var stack []flameFrame
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, flameFrame{spanID: roots[i], level: 0})
	}
	var frames []FlamegraphFrame
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n, ok := nodes[top.spanID]
		if !ok {
			continue
		}
		selfMs := n.row.DurationMs - childSum[n.row.SpanID]
		if selfMs < 0 {
			selfMs = 0
		}
		frames = append(frames, FlamegraphFrame{
			SpanID:     n.row.SpanID,
			Name:       n.row.ServiceName + " :: " + n.row.OperationName,
			Service:    n.row.ServiceName,
			Operation:  n.row.OperationName,
			DurationMs: n.row.DurationMs,
			SelfTimeMs: selfMs,
			Level:      top.level,
			SpanKind:   n.row.SpanKind,
			HasError:   n.row.HasError,
		})
		for i := len(n.children) - 1; i >= 0; i-- {
			stack = append(stack, flameFrame{spanID: n.children[i], level: top.level + 1})
		}
	}
	return frames
}
