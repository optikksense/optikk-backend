package explorer

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// allowedSpanFields is the whitelist of fields users can GROUP BY on spans.
var allowedSpanFields = map[string]string{
	"service_name":       "s.service_name",
	"name":               "s.name",
	"status_code_string": "s.status_code_string",
	"http_route":         "s.mat_http_route",
	"http_method":        "s.http_method",
	"kind_string":        "s.kind_string",
	"db_system":          "s.mat_db_system",
	"db_name":            "s.mat_db_name",
	"rpc_service":        "s.mat_rpc_service",
	"peer_service":       "s.mat_peer_service",
	"host_name":          "s.mat_host_name",
	"k8s_pod_name":       "s.mat_k8s_pod_name",
}

// allowedLogFields is the whitelist of fields users can GROUP BY on logs.
var allowedLogFields = map[string]string{
	"service":       "l.service",
	"severity_text": "l.severity_text",
	"host":          "l.host",
	"pod":           "l.pod",
	"container":     "l.container",
	"environment":   "l.environment",
}

// allowedMetricFields is the whitelist of fields users can GROUP BY on metrics.
var allowedMetricFields = map[string]string{
	"metric_name": "m.metric_name",
	"service":     "m.service",
	"host":        "m.host",
	"environment": "m.environment",
	"unit":        "m.unit",
}

// Repository defines the data access contract for exploration queries.
type Repository interface {
	QueryTimeSeries(teamID, startMs, endMs int64, req ExploreRequest) ([]map[string]any, error)
	QueryTable(teamID, startMs, endMs int64, req ExploreRequest) ([]map[string]any, error)
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// QueryTimeSeries returns time-bucketed aggregation rows.
func (r *ClickHouseRepository) QueryTimeSeries(teamID, startMs, endMs int64, req ExploreRequest) ([]map[string]any, error) {
	q, err := r.buildQuery(teamID, startMs, endMs, req, true)
	if err != nil {
		return nil, err
	}
	return dbutil.QueryMaps(r.db, q)
}

// QueryTable returns tabular aggregation rows (no time bucketing).
func (r *ClickHouseRepository) QueryTable(teamID, startMs, endMs int64, req ExploreRequest) ([]map[string]any, error) {
	q, err := r.buildQuery(teamID, startMs, endMs, req, false)
	if err != nil {
		return nil, err
	}
	return dbutil.QueryMaps(r.db, q)
}

func (r *ClickHouseRepository) buildQuery(teamID, startMs, endMs int64, req ExploreRequest, timeseries bool) (string, error) {
	switch req.SignalType {
	case "span":
		return r.buildSpanQuery(teamID, startMs, endMs, req, timeseries)
	case "log":
		return r.buildLogQuery(teamID, startMs, endMs, req, timeseries)
	case "metric":
		return r.buildMetricQuery(teamID, startMs, endMs, req, timeseries)
	default:
		return "", fmt.Errorf("unsupported signal type: %s", req.SignalType)
	}
}

func (r *ClickHouseRepository) buildSpanQuery(teamID, startMs, endMs int64, req ExploreRequest, timeseries bool) (string, error) {
	agg := spanAggExpr(req.Aggregation)
	groupByCol, err := resolveGroupBy(req.GroupBy, allowedSpanFields)
	if err != nil {
		return "", err
	}

	selectCols := fmt.Sprintf("%s AS value", agg)
	groupByCols := ""
	orderBy := "value DESC"

	if timeseries {
		bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
		selectCols = fmt.Sprintf("%s AS ts, %s", bucket, selectCols)
		groupByCols = "ts"
		orderBy = "ts"
	}

	if groupByCol != "" {
		alias := "group_key"
		selectCols = fmt.Sprintf("%s AS %s, %s", groupByCol, alias, selectCols)
		if groupByCols != "" {
			groupByCols += ", " + alias
		} else {
			groupByCols = alias
		}
	}

	where := fmt.Sprintf(
		"s.team_id = %d AND s.ts_bucket_start >= %d AND s.timestamp >= %s AND s.timestamp <= %s",
		teamID,
		timebucket.SpansBucketStart(startMs/1000),
		dbutil.SqlTime(startMs).Format("'2006-01-02 15:04:05'"),
		dbutil.SqlTime(endMs).Format("'2006-01-02 15:04:05'"),
	)
	where += serviceFilter(req.FilterService, "s.service_name")
	where += operationFilter(req.FilterOperation, "s.name")

	groupClause := ""
	if groupByCols != "" {
		groupClause = "GROUP BY " + groupByCols
	}

	limit := ""
	if !timeseries {
		limit = "LIMIT 100"
	}

	return fmt.Sprintf(
		"SELECT %s FROM observability.spans s WHERE %s %s ORDER BY %s %s",
		selectCols, where, groupClause, orderBy, limit,
	), nil
}

func (r *ClickHouseRepository) buildLogQuery(teamID, startMs, endMs int64, req ExploreRequest, timeseries bool) (string, error) {
	agg := "COUNT(*)"
	groupByCol, err := resolveGroupBy(req.GroupBy, allowedLogFields)
	if err != nil {
		return "", err
	}

	selectCols := fmt.Sprintf("%s AS value", agg)
	groupByCols := ""
	orderBy := "value DESC"

	if timeseries {
		bucket := timebucket.ExprForColumn(startMs, endMs, "toDateTime64(l.timestamp / 1000000000, 9)")
		selectCols = fmt.Sprintf("%s AS ts, %s", bucket, selectCols)
		groupByCols = "ts"
		orderBy = "ts"
	}

	if groupByCol != "" {
		selectCols = fmt.Sprintf("%s AS group_key, %s", groupByCol, selectCols)
		if groupByCols != "" {
			groupByCols += ", group_key"
		} else {
			groupByCols = "group_key"
		}
	}

	startSec := startMs / 1000
	endSec := endMs / 1000
	where := fmt.Sprintf(
		"l.team_id = %d AND l.ts_bucket_start >= %d AND l.ts_bucket_start <= %d AND l.timestamp >= %d AND l.timestamp <= %d",
		teamID, startSec, endSec, startMs*1000000, endMs*1000000,
	)
	where += serviceFilter(req.FilterService, "l.service")

	groupClause := ""
	if groupByCols != "" {
		groupClause = "GROUP BY " + groupByCols
	}

	limit := ""
	if !timeseries {
		limit = "LIMIT 100"
	}

	return fmt.Sprintf(
		"SELECT %s FROM observability.logs l WHERE %s %s ORDER BY %s %s",
		selectCols, where, groupClause, orderBy, limit,
	), nil
}

func (r *ClickHouseRepository) buildMetricQuery(teamID, startMs, endMs int64, req ExploreRequest, timeseries bool) (string, error) {
	agg := metricAggExpr(req.Aggregation)
	groupByCol, err := resolveGroupBy(req.GroupBy, allowedMetricFields)
	if err != nil {
		return "", err
	}

	selectCols := fmt.Sprintf("%s AS value", agg)
	groupByCols := ""
	orderBy := "value DESC"

	if timeseries {
		bucket := timebucket.ExprForColumn(startMs, endMs, "m.timestamp")
		selectCols = fmt.Sprintf("%s AS ts, %s", bucket, selectCols)
		groupByCols = "ts"
		orderBy = "ts"
	}

	if groupByCol != "" {
		selectCols = fmt.Sprintf("%s AS group_key, %s", groupByCol, selectCols)
		if groupByCols != "" {
			groupByCols += ", group_key"
		} else {
			groupByCols = "group_key"
		}
	}

	where := fmt.Sprintf(
		"m.team_id = %d AND m.timestamp >= %s AND m.timestamp <= %s",
		teamID,
		dbutil.SqlTime(startMs).Format("'2006-01-02 15:04:05'"),
		dbutil.SqlTime(endMs).Format("'2006-01-02 15:04:05'"),
	)
	where += serviceFilter(req.FilterService, "m.service")

	groupClause := ""
	if groupByCols != "" {
		groupClause = "GROUP BY " + groupByCols
	}

	limit := ""
	if !timeseries {
		limit = "LIMIT 100"
	}

	return fmt.Sprintf(
		"SELECT %s FROM observability.metrics m WHERE %s %s ORDER BY %s %s",
		selectCols, where, groupClause, orderBy, limit,
	), nil
}

// --- helpers ---

func resolveGroupBy(field string, whitelist map[string]string) (string, error) {
	if field == "" {
		return "", nil
	}
	col, ok := whitelist[field]
	if !ok {
		return "", fmt.Errorf("invalid groupBy field: %s", field)
	}
	return col, nil
}

func spanAggExpr(agg string) string {
	switch agg {
	case "avg":
		return "AVG(s.duration_nano / 1000000.0)"
	case "p95":
		return "quantile(0.95)(s.duration_nano / 1000000.0)"
	case "p99":
		return "quantile(0.99)(s.duration_nano / 1000000.0)"
	default:
		return "COUNT(*)"
	}
}

func metricAggExpr(agg string) string {
	switch agg {
	case "avg":
		return "AVG(m.value)"
	case "sum":
		return "SUM(m.value)"
	case "p95":
		return "quantile(0.95)(m.value)"
	case "p99":
		return "quantile(0.99)(m.value)"
	default:
		return "COUNT(*)"
	}
}

func serviceFilter(svc, col string) string {
	if svc == "" {
		return ""
	}
	return fmt.Sprintf(" AND %s = '%s'", col, svc)
}

func operationFilter(op, col string) string {
	if op == "" {
		return ""
	}
	return fmt.Sprintf(" AND %s = '%s'", col, op)
}
