package tracedetail

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// reNumberLiteral replaces numeric literals (integers and decimals) with ?.
var reNumberLiteral = regexp.MustCompile(`\b\d+(\.\d+)?\b`)

// reStringLiteral replaces single-quoted string literals with ?.
var reStringLiteral = regexp.MustCompile(`'[^']*'`)

// reMultiSpace collapses multiple whitespace characters into a single space.
var reMultiSpace = regexp.MustCompile(`\s+`)

// normalizeDBStatement strips literal values from a SQL statement and returns a canonical fingerprint.
// Example: "SELECT * FROM users WHERE id = 42 AND name = 'Alice'" → "SELECT * FROM users WHERE id = ? AND name = ?"
func normalizeDBStatement(stmt string) string {
	if stmt == "" {
		return ""
	}
	s := reStringLiteral.ReplaceAllString(stmt, "?")
	s = reNumberLiteral.ReplaceAllString(s, "?")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// Repository defines data access for trace detail endpoints.
type Repository interface {
	GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(teamUUID, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error)
	GetSpanAttributes(teamUUID, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(teamUUID, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new trace detail repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetSpanEvents returns span-level events for a trace from observability.spans.
func (r *ClickHouseRepository) GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.timestamp, event_name
		FROM observability.spans s
		ARRAY JOIN s.events AS event_name
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	events := make([]SpanEvent, 0, len(rows))
	seenExceptionEvent := make(map[string]bool, len(rows))
	for _, row := range rows {
		spanID := dbutil.StringFromAny(row["span_id"])
		eventName := dbutil.StringFromAny(row["event_name"])
		if eventName == "exception" {
			seenExceptionEvent[spanID] = true
		}
		events = append(events, SpanEvent{
			SpanID:     spanID,
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			EventName:  eventName,
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
			Attributes: "{}",
		})
	}

	exceptionRows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.timestamp, s.exception_type, s.exception_message, s.exception_stacktrace
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		  AND (s.exception_type != '' OR s.exception_message != '' OR s.exception_stacktrace != '')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}
	for _, row := range exceptionRows {
		spanID := dbutil.StringFromAny(row["span_id"])
		if seenExceptionEvent[spanID] {
			continue
		}
		attrs := map[string]string{}
		if v := dbutil.StringFromAny(row["exception_type"]); v != "" {
			attrs["exception.type"] = v
		}
		if v := dbutil.StringFromAny(row["exception_message"]); v != "" {
			attrs["exception.message"] = v
		}
		if v := dbutil.StringFromAny(row["exception_stacktrace"]); v != "" {
			attrs["exception.stacktrace"] = v
		}
		attrJSON := "{}"
		if len(attrs) > 0 {
			if b, marshalErr := json.Marshal(attrs); marshalErr == nil {
				attrJSON = string(b)
			}
		}
		events = append(events, SpanEvent{
			SpanID:     spanID,
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			EventName:  "exception",
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
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

// GetCriticalPath computes the critical (longest root→leaf) path using subtree end-time pruning.
// ClickHouse computes each span's subtree_end (max end_time within subtree); we then walk
// from each root downward always choosing the child whose subtree ends latest — O(depth) vs O(2^depth).
func (r *ClickHouseRepository) GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error) {
	// Step 1: fetch all spans with start/end times from ClickHouse.
	// ClickHouse also computes each span's subtree_max_end via a correlated subquery.
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
		endNs      int64
		subtreeEnd int64 // max end_ns in subtree — computed below
		children   []string
	}

	nodes := make(map[string]*node, len(rows))
	var roots []string
	for _, row := range rows {
		sid := dbutil.StringFromAny(row["span_id"])
		pid := dbutil.StringFromAny(row["parent_span_id"])
		endNs := dbutil.Int64FromAny(row["end_ns"])
		nodes[sid] = &node{
			spanID:     sid,
			parentID:   pid,
			operation:  dbutil.StringFromAny(row["operation_name"]),
			service:    dbutil.StringFromAny(row["service_name"]),
			durationMs: dbutil.Float64FromAny(row["duration_ms"]),
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

	// Post-order DFS: compute subtreeEnd for each node (max of own endNs and children's subtreeEnd)
	var computeSubtree func(spanID string)
	computeSubtree = func(spanID string) {
		n, ok := nodes[spanID]
		if !ok {
			return
		}
		for _, cid := range n.children {
			computeSubtree(cid)
			if child, ok2 := nodes[cid]; ok2 && child.subtreeEnd > n.subtreeEnd {
				n.subtreeEnd = child.subtreeEnd
			}
		}
	}
	for _, root := range roots {
		computeSubtree(root)
	}

	// Greedy walk: from the root with largest subtreeEnd, always pick the child with largest subtreeEnd
	var bestRoot string
	var bestEnd int64
	for _, root := range roots {
		if n, ok := nodes[root]; ok && n.subtreeEnd > bestEnd {
			bestEnd = n.subtreeEnd
			bestRoot = root
		}
	}

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
		for _, cid := range n.children {
			if child, ok2 := nodes[cid]; ok2 && child.subtreeEnd > bestChildEnd {
				bestChildEnd = child.subtreeEnd
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

// GetSpanSelfTimes returns self_time = duration - SUM(child durations) per span.
// Computed in ClickHouse via a self-join to avoid loading all spans into Go memory.
func (r *ClickHouseRepository) GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error) {
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
		LIMIT 10000
	`, teamUUID, traceID, teamUUID, traceID)
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

// GetErrorPath returns the ERROR-status span chain from root to the deepest error leaf.
func (r *ClickHouseRepository) GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name AS service_name, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
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

// GetSpanAttributes returns the full attribute map for a single span.
func (r *ClickHouseRepository) GetSpanAttributes(teamUUID, traceID, spanID string) (*SpanAttributes, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.attributes_string, s.resource_attributes,
		       s.exception_type, s.exception_message, s.exception_stacktrace,
		       s.db_system, s.db_name, s.db_statement
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ? AND s.span_id = ?
		LIMIT 1
	`, teamUUID, traceID, spanID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	row := rows[0]

	attrString, _ := row["attributes_string"].(map[string]string)
	resourceAttrs, _ := row["resource_attributes"].(map[string]string)
	dbStmt := dbutil.StringFromAny(row["db_statement"])

	// Build a merged attributes map for convenience (resource attrs take lower priority)
	merged := make(map[string]string, len(attrString)+len(resourceAttrs))
	for k, v := range resourceAttrs {
		merged[k] = v
	}
	for k, v := range attrString {
		merged[k] = v
	}

	return &SpanAttributes{
		SpanID:              dbutil.StringFromAny(row["span_id"]),
		TraceID:             dbutil.StringFromAny(row["trace_id"]),
		OperationName:       dbutil.StringFromAny(row["operation_name"]),
		ServiceName:         dbutil.StringFromAny(row["service_name"]),
		AttributesString:    attrString,
		ResourceAttrs:       resourceAttrs,
		Attributes:          merged,
		ExceptionType:       dbutil.StringFromAny(row["exception_type"]),
		ExceptionMessage:    dbutil.StringFromAny(row["exception_message"]),
		ExceptionStacktrace: dbutil.StringFromAny(row["exception_stacktrace"]),
		DBSystem:            dbutil.StringFromAny(row["db_system"]),
		DBName:              dbutil.StringFromAny(row["db_name"]),
		DBStatement:         dbStmt,
		DBStatementNormalized: normalizeDBStatement(dbStmt),
	}, nil
}

// GetRelatedTraces returns root spans with the same service+operation in the given time window,
// excluding the current traceID.
func (r *ClickHouseRepository) GetRelatedTraces(teamUUID, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	startBucket := uint32(startMs / 1000)
	endBucket := uint32(endMs / 1000)
	startNs := uint64(startMs) * 1_000_000
	endNs := uint64(endMs) * 1_000_000

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status, s.timestamp AS start_time
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND s.parent_span_id = ''
		  AND s.service_name = ?
		  AND s.name = ?
		  AND s.trace_id != ?
		ORDER BY s.timestamp DESC
		LIMIT ?
	`, teamUUID, startBucket, endBucket, startNs, endNs,
		serviceName, operationName, excludeTraceID, limit)
	if err != nil {
		return nil, err
	}

	result := make([]RelatedTrace, 0, len(rows))
	for _, row := range rows {
		result = append(result, RelatedTrace{
			TraceID:       dbutil.StringFromAny(row["trace_id"]),
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			Status:        dbutil.StringFromAny(row["status"]),
			StartTime:     dbutil.TimeFromAny(row["start_time"]),
		})
	}
	return result, nil
}
