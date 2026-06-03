package servicemap

import (
	"context"
	"log/slog"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/modules/services/topology"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetServiceMap builds the per-trace service map and returns it in the shared
// topology shape. Edges come from this trace's parent→child span links; p95/p99
// are left at zero (a single trace has no latency distribution) and the frontend
// layers in RED-metric percentiles per service.
func (s *Service) GetServiceMap(ctx context.Context, teamID int64, traceID string) (topology.TopologyResponse, error) {
	rows, err := s.repo.GetServiceMapSpans(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "servicemap: GetServiceMap failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return topology.TopologyResponse{}, err
	}
	return topology.BuildGraph(nodeAggsFromSpans(rows), edgeAggsFromSpans(rows)), nil
}

// GetTraceErrors groups error spans by exception_type (or status_message fallback).
func (s *Service) GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]TraceErrorGroup, error) {
	rows, err := s.repo.GetTraceErrors(ctx, teamID, traceID)
	if err != nil {
		slog.ErrorContext(ctx, "servicemap: GetTraceErrors failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return groupErrors(rows), nil
}

// nodeAggsFromSpans aggregates this trace's spans per service. P50Ms holds the
// running total during aggregation, then is converted to the mean; p95/p99 are
// left at zero for the frontend to fill from RED metrics.
func nodeAggsFromSpans(rows []serviceMapSpanRow) []topology.NodeAgg {
	aggMap := make(map[string]*topology.NodeAgg)
	for i := range rows {
		r := &rows[i]
		if r.ServiceName == "" {
			continue
		}
		a, ok := aggMap[r.ServiceName]
		if !ok {
			a = &topology.NodeAgg{Service: r.ServiceName}
			aggMap[r.ServiceName] = a
		}
		a.RequestCount++
		a.P50Ms += r.DurationMs
		if r.HasError {
			a.ErrorCount++
		}
	}
	out := make([]topology.NodeAgg, 0, len(aggMap))
	for _, a := range aggMap {
		if a.RequestCount > 0 {
			a.P50Ms /= float64(a.RequestCount)
		}
		out = append(out, *a)
	}
	return out
}

// edgeAggsFromSpans derives service→service edges from parent→child span links.
func edgeAggsFromSpans(rows []serviceMapSpanRow) []topology.EdgeAgg {
	bySpan := make(map[string]*serviceMapSpanRow, len(rows))
	for i := range rows {
		bySpan[rows[i].SpanID] = &rows[i]
	}
	aggMap := make(map[[2]string]*topology.EdgeAgg)
	for i := range rows {
		child := &rows[i]
		parent := bySpan[child.ParentSpanID]
		if parent == nil || parent.ServiceName == "" || child.ServiceName == "" || parent.ServiceName == child.ServiceName {
			continue
		}
		key := [2]string{parent.ServiceName, child.ServiceName}
		a, ok := aggMap[key]
		if !ok {
			a = &topology.EdgeAgg{Source: parent.ServiceName, Target: child.ServiceName}
			aggMap[key] = a
		}
		a.CallCount++
		a.P50Ms += child.DurationMs
		if child.HasError {
			a.ErrorCount++
		}
	}
	out := make([]topology.EdgeAgg, 0, len(aggMap))
	for _, a := range aggMap {
		if a.CallCount > 0 {
			a.P50Ms /= float64(a.CallCount)
		}
		out = append(out, *a)
	}
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
			SpanID:           r.SpanID,
			ServiceName:      r.ServiceName,
			OperationName:    r.OperationName,
			ExceptionMessage: r.ExceptionMessage,
			StatusMessage:    r.StatusMessage,
			StartTime:        r.StartTime,
			DurationMs:       r.DurationMs,
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
