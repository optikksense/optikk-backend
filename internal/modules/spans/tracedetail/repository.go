package tracedetail

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository defines data access for trace detail endpoints.
type Repository interface {
	GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(teamUUID, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error)
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new trace detail repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetSpanEvents returns all SpanEvents for a trace, querying the span_events table.
func (r *ClickHouseRepository) GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT e.span_id, e.trace_id, e.name AS event_name, e.timestamp, e.attributes
		FROM observability.span_events e
		WHERE e.team_id = ? AND e.trace_id = ?
		ORDER BY e.timestamp ASC
		LIMIT 1000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	events := make([]SpanEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, SpanEvent{
			SpanID:     dbutil.StringFromAny(row["span_id"]),
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			EventName:  dbutil.StringFromAny(row["event_name"]),
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
			Attributes: dbutil.StringFromAny(row["attributes"]),
		})
	}
	return events, nil
}

// GetSpanKindBreakdown returns total duration and count grouped by span.kind for a trace.
func (r *ClickHouseRepository) GetSpanKindBreakdown(teamUUID, traceID string) ([]SpanKindDuration, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       count()                            AS span_count
		FROM observability.spans
		WHERE team_id = ? AND trace_id = ?
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	// Compute pct_of_trace
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

// GetCriticalPath computes the critical (longest root→leaf) path by walking the span tree
// in Go after fetching all spans for the trace. Returns spans on that path ordered root→leaf.
func (r *ClickHouseRepository) GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       r.service_name, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 10000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	type node struct {
		spanID     string
		parentID   string
		operation  string
		service    string
		durationMs float64
		children   []string
	}

	nodes := make(map[string]*node, len(rows))
	var roots []string
	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		pid := dbutil.StringFromAny(row["parent_span_id"])
		nodes[sid] = &node{
			spanID:     sid,
			parentID:   pid,
			operation:  dbutil.StringFromAny(row["operation_name"]),
			service:    dbutil.StringFromAny(row["service_name"]),
			durationMs: dbutil.Float64FromAny(row["duration_ms"]),
		}
		if pid == "" {
			roots = append(roots, sid)
		}
	}
	// Build children lists
	for sid, n := range nodes {
		if n.parentID != "" {
			if parent, ok := nodes[n.parentID]; ok {
				parent.children = append(parent.children, sid)
			}
		}
		_ = sid
	}

	// DFS to find path with max total duration (sum of durations along root→leaf)
	var bestPath []string
	var bestDuration float64

	var dfs func(spanID string, path []string, accumulated float64)
	dfs = func(spanID string, path []string, accumulated float64) {
		n, ok := nodes[spanID]
		if !ok {
			return
		}
		newPath := append(path, spanID)
		newAccum := accumulated + n.durationMs
		if len(n.children) == 0 {
			if newAccum > bestDuration {
				bestDuration = newAccum
				bestPath = make([]string, len(newPath))
				copy(bestPath, newPath)
			}
			return
		}
		for _, child := range n.children {
			dfs(child, newPath, newAccum)
		}
	}

	for _, root := range roots {
		dfs(root, nil, 0)
	}

	result := make([]CriticalPathSpan, 0, len(bestPath))
	for _, sid := range bestPath {
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

// GetSpanSelfTimes returns self_time = duration - SUM(child durations) per span.
func (r *ClickHouseRepository) GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 10000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	type spanRow struct {
		spanID     string
		parentID   string
		operation  string
		durationMs float64
	}
	spans := make([]spanRow, 0, len(rows))
	childDuration := make(map[string]float64)

	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		pid := dbutil.StringFromAny(row["parent_span_id"])
		d := dbutil.Float64FromAny(row["duration_ms"])
		spans = append(spans, spanRow{
			spanID:     sid,
			parentID:   pid,
			operation:  dbutil.StringFromAny(row["operation_name"]),
			durationMs: d,
		})
		if pid != "" {
			childDuration[pid] += d
		}
	}

	result := make([]SpanSelfTime, 0, len(spans))
	for _, s := range spans {
		childMs := childDuration[s.spanID]
		selfMs := s.durationMs - childMs
		if selfMs < 0 {
			selfMs = 0
		}
		result = append(result, SpanSelfTime{
			SpanID:        s.spanID,
			OperationName: s.operation,
			TotalDuraMs:   s.durationMs,
			SelfTimeMs:    selfMs,
			ChildTimeMs:   childMs,
		})
	}
	return result, nil
}

// GetErrorPath returns the ERROR-status span chain from root to the deepest error leaf.
func (r *ClickHouseRepository) GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       r.service_name, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint
		WHERE s.team_id = ? AND s.trace_id = ?
		  AND (s.has_error = true OR s.status_code_string = 'ERROR')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	// Build a set of error span IDs
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

	// Walk from deepest error span upward to root (following parentID chain)
	// Find a leaf error span (one whose span_id is not anyone's parent in error set)
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
		// No errors found
		return []ErrorPathSpan{}, nil
	}

	// Trace upward
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

	// Reverse so it reads root → leaf
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}
