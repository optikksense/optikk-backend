package tracedetail

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
)

type Service interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]ErrorPathSpan, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]FlamegraphFrame, error)
	GetTraceLogs(ctx context.Context, teamID int64, traceID string) (*TraceLogsResponse, error)
}

type TraceDetailService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &TraceDetailService{repo: repo}
}

func (s *TraceDetailService) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]SpanEvent, error) {
	eventRows, exceptionRows, err := s.repo.GetSpanEvents(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetSpanEvents failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}

	events := make([]SpanEvent, 0, len(eventRows))
	seenException := make(map[string]bool, len(eventRows))
	for _, row := range eventRows {
		name, attrJSON := parseEventJSON(row.EventJSON)
		if name == "exception" {
			seenException[row.SpanID] = true
		}
		events = append(events, SpanEvent{
			SpanID:     row.SpanID,
			TraceID:    row.TraceID,
			EventName:  name,
			Timestamp:  row.Timestamp,
			Attributes: attrJSON,
		})
	}

	for _, row := range exceptionRows {
		if seenException[row.SpanID] {
			continue
		}
		attrs := map[string]string{}
		if row.ExceptionType != "" {
			attrs["exception.type"] = row.ExceptionType
		}
		if row.ExceptionMessage != "" {
			attrs["exception.message"] = row.ExceptionMessage
		}
		if row.ExceptionStacktrace != "" {
			attrs["exception.stacktrace"] = row.ExceptionStacktrace
		}
		attrJSON := "{}"
		if len(attrs) > 0 {
			if b, marshalErr := json.Marshal(attrs); marshalErr == nil {
				attrJSON = string(b)
			}
		}
		events = append(events, SpanEvent{
			SpanID:     row.SpanID,
			TraceID:    row.TraceID,
			EventName:  "exception",
			Timestamp:  row.Timestamp,
			Attributes: attrJSON,
		})
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].Timestamp.Equal(events[j].Timestamp) {
			if events[i].SpanID == events[j].SpanID {
				return events[i].EventName < events[j].EventName
			}
			return events[i].SpanID < events[j].SpanID
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
	return events, nil
}

func (s *TraceDetailService) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]SpanKindDuration, error) {
	rows, err := s.repo.GetSpanKindBreakdown(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetSpanKindBreakdown failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}

	var totalMs float64
	result := make([]SpanKindDuration, len(rows))
	for i, row := range rows {
		totalMs += row.TotalDuraMs
		result[i] = SpanKindDuration{
			SpanKind:    row.SpanKind,
			TotalDuraMs: row.TotalDuraMs,
			SpanCount:   row.SpanCount,
		}
	}
	if totalMs > 0 {
		for i := range result {
			result[i].PctOfTrace = result[i].TotalDuraMs * 100.0 / totalMs
		}
	}
	return result, nil
}

func (s *TraceDetailService) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]CriticalPathSpan, error) {
	rows, err := s.repo.GetCriticalPath(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetCriticalPath failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildCriticalPath(rows), nil
}

func (s *TraceDetailService) GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error) {
	return s.repo.GetSpanSelfTimes(ctx, teamID, traceID)
}

func (s *TraceDetailService) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]ErrorPathSpan, error) {
	rows, err := s.repo.GetErrorPath(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetErrorPath failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildErrorPath(rows), nil
}

func (s *TraceDetailService) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*SpanAttributes, error) {
	row, err := s.repo.GetSpanAttributes(ctx, teamID, traceID, spanID)
	if err != nil {
		slog.Error("tracedetail: GetSpanAttributes failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID), slog.String("span_id", spanID))
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	merged := make(map[string]string, len(row.AttributesString)+len(row.ResourceAttrs))
	for k, v := range row.ResourceAttrs {
		merged[k] = v
	}
	for k, v := range row.AttributesString {
		merged[k] = v
	}

	return &SpanAttributes{
		SpanID:                row.SpanID,
		TraceID:               row.TraceID,
		OperationName:         row.OperationName,
		ServiceName:           row.ServiceName,
		AttributesString:      row.AttributesString,
		ResourceAttrs:         row.ResourceAttrs,
		Attributes:            merged,
		ExceptionType:         row.ExceptionType,
		ExceptionMessage:      row.ExceptionMessage,
		ExceptionStacktrace:   row.ExceptionStacktrace,
		DBSystem:              row.DBSystem,
		DBName:                row.DBName,
		DBStatement:           row.DBStatement,
		DBStatementNormalized: normalizeDBStatement(row.DBStatement),
	}, nil
}

func (s *TraceDetailService) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(ctx, teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}

func (s *TraceDetailService) GetTraceLogs(ctx context.Context, teamID int64, traceID string) (*TraceLogsResponse, error) {
	rows, err := s.repo.GetTraceLogs(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetTraceLogs failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	logs := make([]TraceLog, len(rows))
	for i, row := range rows {
		logs[i] = TraceLog{
			Timestamp:         uint64(row.Timestamp.UnixNano()),
			ObservedTimestamp: row.ObservedTimestamp,
			SeverityText:      row.SeverityText,
			SeverityNumber:    row.SeverityNumber,
			Body:              row.Body,
			TraceID:           row.TraceID,
			SpanID:            row.SpanID,
			TraceFlags:        row.TraceFlags,
			ServiceName:       row.ServiceName,
			Host:              row.Host,
			Pod:               row.Pod,
			Container:         row.Container,
			Environment:       row.Environment,
			AttributesString:  row.AttributesString,
			AttributesNumber:  row.AttributesNumber,
			AttributesBool:    row.AttributesBool,
			ScopeName:         row.ScopeName,
			ScopeVersion:      row.ScopeVersion,
		}
	}
	return &TraceLogsResponse{
		Logs:          logs,
		IsSpeculative: false,
	}, nil
}

func (s *TraceDetailService) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]FlamegraphFrame, error) {
	rows, err := s.repo.GetFlamegraphData(ctx, teamID, traceID)
	if err != nil {
		slog.Error("tracedetail: GetFlamegraphData failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("trace_id", traceID))
		return nil, err
	}
	return buildFlamegraph(rows), nil
}

// parseEventJSON extracts the event name and attributes JSON from an event
// string stored in the events column. New format events are JSON objects:
//
//	{"name":"...","timeUnixNano":"...","attributes":{...}}
//
// Legacy events are plain strings containing just the event name.
func parseEventJSON(raw string) (name string, attrs string) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 0 {
		return "", "{}"
	}
	if raw[0] != '{' {
		return raw, "{}"
	}
	var obj struct {
		Name       string            `json:"name"`
		Attributes map[string]string `json:"attributes"`
	}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw, "{}"
	}
	if len(obj.Attributes) == 0 {
		return obj.Name, "{}"
	}
	b, err := json.Marshal(obj.Attributes)
	if err != nil {
		return obj.Name, "{}"
	}
	return obj.Name, string(b)
}

// buildCriticalPath runs the longest-path graph algorithm on the raw DB rows.
func buildCriticalPath(rows []criticalPathRow) []CriticalPathSpan {
	type node struct {
		row        criticalPathRow
		subtreeEnd int64
		children   []string
	}

	nodes := make(map[string]*node, len(rows))
	var roots []string
	for _, row := range rows {
		nodes[row.SpanID] = &node{row: row, subtreeEnd: row.EndNs}
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

	var bestRoot string
	var bestEnd int64
	for _, root := range roots {
		if n := nodes[root]; n.subtreeEnd > bestEnd {
			bestEnd = n.subtreeEnd
			bestRoot = root
		}
	}

	var result []CriticalPathSpan
	cur := bestRoot
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
		var bestChild string
		var bestChildEnd, bestChildStart int64
		for _, cid := range n.children {
			child := nodes[cid]
			if child.subtreeEnd > bestChildEnd || (child.subtreeEnd == bestChildEnd && child.row.StartNs > bestChildStart) {
				bestChildEnd = child.subtreeEnd
				bestChildStart = child.row.StartNs
				bestChild = cid
			}
		}
		cur = bestChild
	}
	return result
}

// buildErrorPath builds the root→leaf error chain from raw error span rows.
func buildErrorPath(rows []errorPathRow) []ErrorPathSpan {
	type eSpan struct {
		row errorPathRow
	}
	spans := make(map[string]*eSpan, len(rows))
	for i := range rows {
		spans[rows[i].SpanID] = &eSpan{row: rows[i]}
	}

	childOf := make(map[string]bool, len(rows))
	for _, s := range spans {
		if s.row.ParentSpanID != "" {
			childOf[s.row.ParentSpanID] = true
		}
	}
	var leafID string
	for sid := range spans {
		if !childOf[sid] {
			leafID = sid
			break
		}
	}
	if leafID == "" {
		return []ErrorPathSpan{}
	}

	var chain []ErrorPathSpan
	cur := leafID
	for cur != "" {
		s, ok := spans[cur]
		if !ok {
			break
		}
		chain = append(chain, ErrorPathSpan{
			SpanID:        s.row.SpanID,
			ParentSpanID:  s.row.ParentSpanID,
			OperationName: s.row.OperationName,
			ServiceName:   s.row.ServiceName,
			Status:        s.row.Status,
			StatusMessage: s.row.StatusMessage,
			StartTime:     s.row.StartTime,
			DurationMs:    s.row.DurationMs,
		})
		cur = s.row.ParentSpanID
	}

	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

// buildFlamegraph builds depth-first flamegraph frames from raw span rows.
func buildFlamegraph(rows []flamegraphRow) []FlamegraphFrame {
	type spanNode struct {
		row      flamegraphRow
		children []string
	}

	nodes := make(map[string]*spanNode, len(rows))
	var roots []string
	for i := range rows {
		row := &rows[i]
		nodes[row.SpanID] = &spanNode{row: *row}
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

	childSum := make(map[string]float64, len(nodes))
	for _, n := range nodes {
		if n.row.ParentSpanID != "" {
			childSum[n.row.ParentSpanID] += n.row.DurationMs
		}
	}

	type dfsFrame struct {
		spanID string
		level  int
	}
	var stack []dfsFrame
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, dfsFrame{spanID: roots[i], level: 0})
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
			stack = append(stack, dfsFrame{spanID: n.children[i], level: top.level + 1})
		}
	}
	return frames
}

