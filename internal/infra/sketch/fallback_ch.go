package sketch

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// CHFallback runs narrow approximate-aggregation queries against
// observability.* tables when the sketch store has no data for a
// (kind, team, range). Each kind maps to a deterministic (table, value-column,
// group-by-columns, filter predicate).
type CHFallback struct {
	db clickhouse.Conn
}

func NewCHFallback(db clickhouse.Conn) *CHFallback { return &CHFallback{db: db} }

type fallbackPlan struct {
	table       string
	valueExpr   string   // float column or expression
	weightExpr  string   // "" → unweighted
	groupBy     []string // dimension columns (concatenated to form dim string with '|')
	filterExpr  string   // additional WHERE (no leading AND)
	tsColumn    string
	idenColumn  string // for cardinality kinds — the column to count distinct
}

func plan(kind Kind) (fallbackPlan, bool) {
	switch kind.ID {
	case SpanLatencyService.ID:
		return fallbackPlan{
			table:     "observability.spans",
			valueExpr: "duration_nano / 1000000.0",
			groupBy:   []string{"service_name"},
			tsColumn:  "timestamp",
		}, true
	case SpanLatencyEndpoint.ID:
		return fallbackPlan{
			table:     "observability.spans",
			valueExpr: "duration_nano / 1000000.0",
			groupBy:   []string{"service_name", "name", "mat_http_route", "http_method"},
			tsColumn:  "timestamp",
		}, true
	case DbOpLatency.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"attributes['db.system']",
				"attributes['db.operation.name']",
				"attributes['db.collection.name']",
				"attributes['db.namespace']",
			},
			filterExpr: "metric_name = 'db.client.operation.duration'",
			tsColumn:   "timestamp",
		}, true
	case KafkaTopicLatency.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"attributes['messaging.destination.name']",
				"attributes['messaging.client.id']",
			},
			filterExpr: "metric_name IN ('messaging.kafka.publish.latency','messaging.kafka.consume.latency')",
			tsColumn:   "timestamp",
		}, true
	case NodePodCount.ID:
		return fallbackPlan{
			table:      "observability.spans",
			idenColumn: "mat_k8s_pod_name",
			groupBy:    []string{"coalesce(host,'')", "attributes['k8s.namespace.name']"},
			filterExpr: "mat_k8s_pod_name != ''",
			tsColumn:   "timestamp",
		}, true
	case AiTraceCount.ID:
		return fallbackPlan{
			table:      "observability.spans",
			idenColumn: "trace_id",
			groupBy:    []string{"service_name"},
			filterExpr: "attributes['gen_ai.system'] != ''",
			tsColumn:   "timestamp",
		}, true
	case HttpServerDuration.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"attributes['otel.scope.name']",
				"attributes['http.route']",
				"attributes['http.request.method']",
				"toString(attributes['http.response.status_code'])",
				"attributes['server.address']",
			},
			filterExpr: "metric_name = 'http.server.request.duration'",
			tsColumn:   "timestamp",
		}, true
	case HttpClientDuration.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"attributes['otel.scope.name']",
				"attributes['server.address']",
				"attributes['http.request.method']",
				"toString(attributes['http.response.status_code'])",
			},
			filterExpr: "metric_name = 'http.client.request.duration'",
			tsColumn:   "timestamp",
		}, true
	case JvmMetricLatency.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"metric_name",
				"attributes['service.name']",
				"attributes['k8s.pod.name']",
			},
			filterExpr: "metric_name LIKE 'jvm.%'",
			tsColumn:   "timestamp",
		}, true
	case DbQueryLatency.ID:
		return fallbackPlan{
			table:      "observability.metrics",
			valueExpr:  "value",
			weightExpr: "hist_count",
			groupBy: []string{
				"attributes['db.system']",
				"attributes['db.query.text.fingerprint']",
			},
			filterExpr: "metric_name = 'db.client.operation.duration' AND attributes['db.query.text.fingerprint'] != ''",
			tsColumn:   "timestamp",
		}, true
	}
	return fallbackPlan{}, false
}

func (f *CHFallback) Quantiles(ctx context.Context, kind Kind, teamID string, startMs, endMs int64, qs ...float64) (map[string][]float64, error) {
	p, ok := plan(kind)
	if !ok || f == nil || f.db == nil {
		return map[string][]float64{}, nil
	}
	qExpr := "quantileTDigest(%f)(%s)"
	if p.weightExpr != "" {
		qExpr = "quantileTDigestWeighted(%f)(%s, " + p.weightExpr + ")"
	}
	selects := make([]string, 0, len(qs)+1)
	selects = append(selects, dimExpr(p.groupBy)+" AS dim")
	for _, q := range qs {
		selects = append(selects, fmt.Sprintf(qExpr, q, p.valueExpr))
	}

	where := fmt.Sprintf("team_id = ? AND %s BETWEEN ? AND ?", p.tsColumn)
	if p.filterExpr != "" {
		where += " AND " + p.filterExpr
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s GROUP BY dim",
		joinStrings(selects, ", "), p.table, where)

	rows, err := f.db.Query(ctx, query, teamID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]float64)
	for rows.Next() {
		dim := ""
		vals := make([]float64, len(qs))
		dest := make([]any, 0, 1+len(qs))
		dest = append(dest, &dim)
		for i := range vals {
			dest = append(dest, &vals[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		out[dim] = vals
	}
	return out, rows.Err()
}

func (f *CHFallback) Uniques(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]uint64, error) {
	p, ok := plan(kind)
	if !ok || f == nil || f.db == nil || p.idenColumn == "" {
		return map[string]uint64{}, nil
	}
	where := fmt.Sprintf("team_id = ? AND %s BETWEEN ? AND ?", p.tsColumn)
	if p.filterExpr != "" {
		where += " AND " + p.filterExpr
	}
	query := fmt.Sprintf("SELECT %s AS dim, uniq(%s) FROM %s WHERE %s GROUP BY dim",
		dimExpr(p.groupBy), p.idenColumn, p.table, where)

	rows, err := f.db.Query(ctx, query, teamID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]uint64)
	for rows.Next() {
		var dim string
		var n uint64
		if err := rows.Scan(&dim, &n); err != nil {
			return nil, err
		}
		out[dim] = n
	}
	return out, rows.Err()
}

func dimExpr(cols []string) string {
	if len(cols) == 0 {
		return "''"
	}
	if len(cols) == 1 {
		return "toString(" + cols[0] + ")"
	}
	parts := make([]string, 0, len(cols)*2-1)
	for i, c := range cols {
		if i > 0 {
			parts = append(parts, "'|'")
		}
		parts = append(parts, "toString("+c+")")
	}
	return "concat(" + joinStrings(parts, ", ") + ")"
}

func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	n := len(sep) * (len(ss) - 1)
	for _, s := range ss {
		n += len(s)
	}
	b := make([]byte, 0, n)
	b = append(b, ss[0]...)
	for _, s := range ss[1:] {
		b = append(b, sep...)
		b = append(b, s...)
	}
	return string(b)
}
