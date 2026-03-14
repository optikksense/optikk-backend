package tracedetail

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *ClickHouseRepository) GetSpanKindBreakdown(teamID int64, traceID string) ([]SpanKindDuration, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       count()                            AS span_count
		FROM observability.spans
		WHERE team_id = ? AND trace_id = ?
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	var totalMs float64
	breakdown := make([]SpanKindDuration, 0, len(rows))
	for _, row := range rows {
		d := dbutil.Float64FromAny(row["total_duration_ms"])
		totalMs += d
		breakdown = append(breakdown, SpanKindDuration{
			SpanKind:    dbutil.StringFromAny(row["span_kind"]),
			TotalDuraMs: d,
			SpanCount:   dbutil.Int64FromAny(row["span_count"]),
		})
	}
	if totalMs > 0 {
		for i := range breakdown {
			breakdown[i].PctOfTrace = breakdown[i].TotalDuraMs * 100.0 / totalMs
		}
	}
	return breakdown, nil
}

// GetCriticalPath computes the critical path through a trace.
// Uses a proper longest-path algorithm: for each span, the critical path
// follows the child whose subtree finishes last (determines the parent's actual end time).
func (r *ClickHouseRepository) GetCriticalPath(teamID int64, traceID string) ([]CriticalPathSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id,
		       s.name AS operation_name,
		       s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       toUnixTimestamp64Nano(s.timestamp) + s.duration_nano AS end_ns
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY start_ns ASC
		LIMIT 5000
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	type node struct {
		spanID     string
		parentID   string
		operation  string
		service    string
		durationMs float64
		startNs    int64
		endNs      int64
		subtreeEnd int64
		children   []string
	}

	nodes := make(map[string]*node, len(rows))
	var roots []string
	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		pid := dbutil.StringFromAny(row["parent_span_id"])
		startNs := dbutil.Int64FromAny(row["start_ns"])
		endNs := dbutil.Int64FromAny(row["end_ns"])
		nodes[sid] = &node{
			spanID:     sid,
			parentID:   pid,
			operation:  dbutil.StringFromAny(row["operation_name"]),
			service:    dbutil.StringFromAny(row["service_name"]),
			durationMs: dbutil.Float64FromAny(row["duration_ms"]),
			startNs:    startNs,
			endNs:      endNs,
			subtreeEnd: endNs,
		}
		if pid == "" {
			roots = append(roots, sid)
		}
	}
	for sid, n := range nodes {
		if n.parentID != "" {
			if parent, ok := nodes[n.parentID]; ok {
				parent.children = append(parent.children, sid)
			}
		}
		_ = sid
	}

	// Iterative post-order traversal to compute subtreeEnd.
	type frame struct {
		spanID   string
		childIdx int
	}
	for _, root := range roots {
		stack := []frame{{spanID: root, childIdx: 0}}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			n, ok := nodes[top.spanID]
			if !ok {
				stack = stack[:len(stack)-1]
				continue
			}
			if top.childIdx < len(n.children) {
				cid := n.children[top.childIdx]
				top.childIdx++
				stack = append(stack, frame{spanID: cid, childIdx: 0})
			} else {
				for _, cid := range n.children {
					if child, ok2 := nodes[cid]; ok2 && child.subtreeEnd > n.subtreeEnd {
						n.subtreeEnd = child.subtreeEnd
					}
				}
				stack = stack[:len(stack)-1]
			}
		}
	}

	// Find the root with the largest subtreeEnd.
	var bestRoot string
	var bestEnd int64
	for _, root := range roots {
		if n, ok := nodes[root]; ok && n.subtreeEnd > bestEnd {
			bestEnd = n.subtreeEnd
			bestRoot = root
		}
	}

	// Walk from root: at each level, pick the child whose subtreeEnd matches
	// the parent's subtreeEnd (the child that actually determines the end time).
	// For ties, prefer the child that starts latest (it contributed more wall-clock time).
	var path []string
	cur := bestRoot
	for cur != "" {
		path = append(path, cur)
		n, ok := nodes[cur]
		if !ok || len(n.children) == 0 {
			break
		}
		var bestChild string
		var bestChildEnd int64
		var bestChildStart int64
		for _, cid := range n.children {
			child, ok2 := nodes[cid]
			if !ok2 {
				continue
			}
			if child.subtreeEnd > bestChildEnd ||
				(child.subtreeEnd == bestChildEnd && child.startNs > bestChildStart) {
				bestChildEnd = child.subtreeEnd
				bestChildStart = child.startNs
				bestChild = cid
			}
		}
		cur = bestChild
	}

	result := make([]CriticalPathSpan, 0, len(path))
	for _, sid := range path {
		n := nodes[sid]
		result = append(result, CriticalPathSpan{
			SpanID:        n.spanID,
			OperationName: n.operation,
			ServiceName:   n.service,
			DurationMs:    n.durationMs,
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetSpanSelfTimes(teamID int64, traceID string) ([]SpanSelfTime, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id,
		       s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS total_duration_ms,
		       coalesce(child_sum.child_ms, 0) AS child_time_ms,
		       greatest(0, s.duration_nano / 1000000.0 - coalesce(child_sum.child_ms, 0)) AS self_time_ms
		FROM observability.spans s
		LEFT JOIN (
		    SELECT parent_span_id, sum(duration_nano) / 1000000.0 AS child_ms
		    FROM observability.spans
		    WHERE team_id = ? AND trace_id = ?
		    GROUP BY parent_span_id
		) AS child_sum ON child_sum.parent_span_id = s.span_id
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY self_time_ms DESC
		LIMIT 5000
	`, uint32(teamID), traceID, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	result := make([]SpanSelfTime, 0, len(rows))
	for _, row := range rows {
		result = append(result, SpanSelfTime{
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			TotalDuraMs:   dbutil.Float64FromAny(row["total_duration_ms"]),
			SelfTimeMs:    dbutil.Float64FromAny(row["self_time_ms"]),
			ChildTimeMs:   dbutil.Float64FromAny(row["child_time_ms"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetErrorPath(teamID int64, traceID string) ([]ErrorPathSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name AS service_name, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		  AND (s.has_error = true OR s.status_code_string = 'ERROR')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	type eSpan struct {
		spanID     string
		parentID   string
		operation  string
		service    string
		status     string
		message    string
		startTime  interface{}
		durationMs float64
	}
	errorSpans := make(map[string]*eSpan, len(rows))
	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		errorSpans[sid] = &eSpan{
			spanID:     sid,
			parentID:   dbutil.StringFromAny(row["parent_span_id"]),
			operation:  dbutil.StringFromAny(row["operation_name"]),
			service:    dbutil.StringFromAny(row["service_name"]),
			status:     dbutil.StringFromAny(row["status"]),
			message:    dbutil.StringFromAny(row["status_message"]),
			startTime:  row["start_time"],
			durationMs: dbutil.Float64FromAny(row["duration_ms"]),
		}
	}

	childOf := make(map[string]bool)
	for _, s := range errorSpans {
		if s.parentID != "" {
			childOf[s.parentID] = true
		}
	}
	var leafID string
	for sid := range errorSpans {
		if !childOf[sid] {
			leafID = sid
			break
		}
	}
	if leafID == "" {
		return []ErrorPathSpan{}, nil
	}

	var chain []ErrorPathSpan
	cur := leafID
	for cur != "" {
		s, ok := errorSpans[cur]
		if !ok {
			break
		}
		chain = append(chain, ErrorPathSpan{
			SpanID:        s.spanID,
			ParentSpanID:  s.parentID,
			OperationName: s.operation,
			ServiceName:   s.service,
			Status:        s.status,
			StatusMessage: s.message,
			StartTime:     dbutil.TimeFromAny(s.startTime),
			DurationMs:    s.durationMs,
		})
		cur = s.parentID
	}

	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// GetFlamegraphData returns span data structured as flamegraph frames.
// Frames are in depth-first order with self-time computed.
func (r *ClickHouseRepository) GetFlamegraphData(teamID int64, traceID string) ([]FlamegraphFrame, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name, s.kind_string AS span_kind,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       s.has_error
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY start_ns ASC
		LIMIT 5000
	`, uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	type spanNode struct {
		spanID     string
		parentID   string
		operation  string
		service    string
		kind       string
		durationMs float64
		hasError   bool
		children   []string
	}

	nodes := make(map[string]*spanNode, len(rows))
	var roots []string
	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		pid := dbutil.StringFromAny(row["parent_span_id"])
		nodes[sid] = &spanNode{
			spanID:     sid,
			parentID:   pid,
			operation:  dbutil.StringFromAny(row["operation_name"]),
			service:    dbutil.StringFromAny(row["service_name"]),
			kind:       dbutil.StringFromAny(row["span_kind"]),
			durationMs: dbutil.Float64FromAny(row["duration_ms"]),
			hasError:   dbutil.BoolFromAny(row["has_error"]),
		}
		if pid == "" {
			roots = append(roots, sid)
		}
	}
	for sid, n := range nodes {
		if n.parentID != "" {
			if p, ok := nodes[n.parentID]; ok {
				p.children = append(p.children, sid)
			}
		}
		_ = sid
	}

	// Compute child time sum for self-time calculation.
	childSum := make(map[string]float64, len(nodes))
	for _, n := range nodes {
		if n.parentID != "" {
			childSum[n.parentID] += n.durationMs
		}
	}

	// Depth-first traversal to produce frames.
	var frames []FlamegraphFrame
	type dfsFrame struct {
		spanID string
		level  int
	}
	var stack []dfsFrame
	for i := len(roots) - 1; i >= 0; i-- {
		stack = append(stack, dfsFrame{spanID: roots[i], level: 0})
	}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n, ok := nodes[top.spanID]
		if !ok {
			continue
		}
		selfMs := n.durationMs - childSum[n.spanID]
		if selfMs < 0 {
			selfMs = 0
		}
		frames = append(frames, FlamegraphFrame{
			SpanID:     n.spanID,
			Name:       n.service + " :: " + n.operation,
			Service:    n.service,
			Operation:  n.operation,
			DurationMs: n.durationMs,
			SelfTimeMs: selfMs,
			Level:      top.level,
			SpanKind:   n.kind,
			HasError:   n.hasError,
		})
		for i := len(n.children) - 1; i >= 0; i-- {
			stack = append(stack, dfsFrame{spanID: n.children[i], level: top.level + 1})
		}
	}
	return frames, nil
}
