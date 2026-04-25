package trace_servicemap	//nolint:revive,stylecheck

import (
	"context"
	"log/slog"
	"sort"
)

type Service interface {
	GetServiceMap(ctx context.Context, teamID int64, traceID string) (ServiceMapResponse, error)
	GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]TraceErrorGroup, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service	{ return &service{repo: repo} }

// GetServiceMap builds the per-trace service map (Datadog-style node+edge graph).
func (s *service) GetServiceMap(ctx context.Context, teamID int64, traceID string) (ServiceMapResponse, error) {
	rows, err := s.repo.GetServiceMapSpans(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "trace_servicemap: GetServiceMap failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return ServiceMapResponse{}, err
	}
	return ServiceMapResponse{Nodes: nodesFromSpans(rows), Edges: edgesFromSpans(rows)}, nil
}

// GetTraceErrors groups error spans by exception_type (or status_message fallback).
func (s *service) GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]TraceErrorGroup, error) {
	rows, err := s.repo.GetTraceErrors(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "trace_servicemap: GetTraceErrors failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return groupErrors(rows), nil
}

func nodesFromSpans(rows []serviceMapSpanRow) []ServiceMapNode {
	nodeMap := make(map[string]*ServiceMapNode)
	for i := range rows {
		r := &rows[i]
		if r.ServiceName == "" {
			continue
		}
		n, ok := nodeMap[r.ServiceName]
		if !ok {
			n = &ServiceMapNode{Service: r.ServiceName}
			nodeMap[r.ServiceName] = n
		}
		n.SpanCount++
		n.TotalMs += r.DurationMs
		if r.HasError {
			n.ErrorCount++
		}
	}
	out := make([]ServiceMapNode, 0, len(nodeMap))
	for _, n := range nodeMap {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TotalMs > out[j].TotalMs })
	return out
}

func edgesFromSpans(rows []serviceMapSpanRow) []ServiceMapEdge {
	bySpan := make(map[string]*serviceMapSpanRow, len(rows))
	for i := range rows {
		bySpan[rows[i].SpanID] = &rows[i]
	}
	edgeMap := make(map[[2]string]*ServiceMapEdge)
	for i := range rows {
		child := &rows[i]
		parent := bySpan[child.ParentSpanID]
		if parent == nil || parent.ServiceName == "" || child.ServiceName == "" || parent.ServiceName == child.ServiceName {
			continue
		}
		key := [2]string{parent.ServiceName, child.ServiceName}
		e, ok := edgeMap[key]
		if !ok {
			e = &ServiceMapEdge{From: parent.ServiceName, To: child.ServiceName}
			edgeMap[key] = e
		}
		e.CallCount++
		e.TotalMs += child.DurationMs
		if child.HasError {
			e.ErrorCount++
		}
	}
	out := make([]ServiceMapEdge, 0, len(edgeMap))
	for _, e := range edgeMap {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CallCount > out[j].CallCount })
	return out
}

func groupErrors(rows []traceErrorRow) []TraceErrorGroup {
	groups := make(map[string]*TraceErrorGroup)
	for _, r := range rows {
		key := errorGroupKey(r)
		g, ok := groups[key]
		if !ok {
			g = &TraceErrorGroup{ExceptionType: key}
			groups[key] = g
		}
		g.Count++
		g.Spans = append(g.Spans, TraceErrorSpan{
			SpanID:			r.SpanID,
			ServiceName:		r.ServiceName,
			OperationName:		r.OperationName,
			ExceptionMessage:	r.ExceptionMessage,
			StatusMessage:		r.StatusMessage,
			StartTime:		r.StartTime,
			DurationMs:		r.DurationMs,
		})
	}
	out := make([]TraceErrorGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out
}

func errorGroupKey(r traceErrorRow) string {
	if r.ExceptionType != "" {
		return r.ExceptionType
	}
	if r.StatusMessage != "" {
		return r.StatusMessage
	}
	return "UnknownError"
}
