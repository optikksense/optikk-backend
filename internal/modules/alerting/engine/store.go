package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
)

const (
	spansRollupPrefix    = "observability.spans_rollup"
	spansByVersionPrefix = "observability.spans_by_version"
	aiSpansRollupV2Prefix = "observability.ai_spans_rollup_v2"
)

// EventStore persists alerting audit events (ClickHouse observability.alert_events).
type EventStore interface {
	WriteEvent(ctx context.Context, ev shared.AlertEvent) error
	ListEvents(ctx context.Context, teamID, alertID int64, limit int) ([]shared.AlertEvent, error)
	ListRecentEvents(ctx context.Context, teamID int64, limit int) ([]shared.AlertEvent, error)
}

// DataSource is the evaluator-side read API — SLO / error rate / AI metric
// queries + deploy correlation. Kept separate from EventStore because the
// evaluator loop and backtest runner consume these without caring about
// event audit persistence.
type DataSource interface {
	SLOErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ServiceErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error)
	AIMetric(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, windowSecs int64) (float64, bool, error)
	AIMetricHistorical(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, fromMs, toMs, windowSecs int64) (float64, bool, error)
	DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]shared.DeployRef, error)
}

// chStore is the single concrete impl that satisfies both EventStore and
// DataSource. Held as one type so wiring only needs two fields (*sql.DB is
// not used here — MySQL persistence lives in rules.Repository).
type chStore struct {
	ch     clickhouse.Conn
	chConn clickhouse.Conn
}

// NewStore constructs the shared ClickHouse-backed event store + data source.
// Callers can type-assert the returned value to EventStore or DataSource.
func NewStore(ch clickhouse.Conn, chConn clickhouse.Conn) *chStore { //nolint:revive // ok
	return &chStore{ch: ch, chConn: chConn}
}

// ---------- EventStore ----------

func (s *chStore) WriteEvent(ctx context.Context, ev shared.AlertEvent) error {
	if s.chConn == nil {
		return nil
	}
	if ev.Ts.IsZero() {
		ev.Ts = time.Now().UTC()
	}
	return s.chConn.Exec(ctx, `
		INSERT INTO observability.alert_events
		(ts, team_id, alert_id, instance_key, kind, from_state, to_state, values, actor_user_id, message, deploy_refs, transition_id)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		ev.Ts, ev.TeamID, ev.AlertID, ev.InstanceKey, ev.Kind, ev.FromState, ev.ToState,
		ev.Values, ev.ActorUserID, ev.Message, ev.DeployRefs, ev.TransitionID,
	)
}

func (s *chStore) ListEvents(ctx context.Context, teamID, alertID int64, limit int) ([]shared.AlertEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	var rows []shared.AlertEvent
	err := s.ch.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT ts, team_id, alert_id, instance_key, kind, from_state, to_state,
		       values, actor_user_id, message, deploy_refs, transition_id
		FROM observability.alert_events
		WHERE team_id = @teamID AND alert_id = @alertID
		ORDER BY ts DESC
		LIMIT @limit`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("alertID", alertID),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (s *chStore) ListRecentEvents(ctx context.Context, teamID int64, limit int) ([]shared.AlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows []shared.AlertEvent
	err := s.ch.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT ts, team_id, alert_id, instance_key, kind, from_state, to_state,
		       values, actor_user_id, message, deploy_refs, transition_id
		FROM observability.alert_events
		WHERE team_id = @teamID
		ORDER BY ts DESC
		LIMIT @limit`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

// ---------- DataSource ----------

func (s *chStore) SLOErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	return s.errorRateLast(ctx, teamID, serviceName, windowSecs)
}

func (s *chStore) ServiceErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	return s.errorRateLast(ctx, teamID, serviceName, windowSecs)
}

func (s *chStore) errorRateLast(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	if windowSecs <= 0 {
		windowSecs = 300
	}
	nowMs := time.Now().UnixMilli()
	startMs := nowMs - windowSecs*1000
	return s.errorRateRange(ctx, teamID, serviceName, startMs, nowMs, dbutil.OverviewCtx)
}

// errorRateRange reads root-span error rate from the spans_rollup cascade.
// Rollup MV already filters on `parent_span_id = '' OR '0000…0000'`, so the
// semantics match the historical raw query.
func (s *chStore) errorRateRange(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, ctxWrap func(context.Context) context.Context) (float64, bool, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toFloat64(sumMerge(request_count)) AS total,
		       toFloat64(sumMerge(error_count))   AS errs
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	if strings.TrimSpace(serviceName) != "" {
		query += ` AND service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	var row struct {
		Total float64 `ch:"total"`
		Errs  float64 `ch:"errs"`
	}
	if err := s.ch.QueryRow(ctxWrap(ctx), query, args...).ScanStruct(&row); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil
	}
	return row.Errs * 100.0 / row.Total, false, nil
}

func (s *chStore) ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs
	return s.errorRateRange(ctx, teamID, serviceName, fromMs, toMs, dbutil.ExplorerCtx)
}

// DeploysInRange reads first-seen per (service, version, environment) from
// `spans_by_version_*`. 1m tier — deploy windows are minute-scale.
// Uses `HAVING first_seen BETWEEN @startNs AND @endNs` so a version that has
// been seen for weeks but emits spans inside the window doesn't register as a
// new deploy (semantic parity with the prior raw `min(timestamp)` + time-range
// filter).
func (s *chStore) DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]shared.DeployRef, error) {
	table, _ := rollup.TierTableFor(spansByVersionPrefix, fromMs, toMs)
	var rows []shared.DeployRef
	err := s.ch.Select(dbutil.OverviewCtx(ctx), &rows, fmt.Sprintf(`
		SELECT service_name                  AS service_name,
		       service_version               AS version,
		       environment                   AS environment,
		       minMerge(first_seen)          AS first_seen
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service_name, version, environment
		HAVING first_seen BETWEEN @start AND @end
		ORDER BY first_seen ASC`, table),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(fromMs)),
		clickhouse.Named("end", time.UnixMilli(toMs)),
	)
	return rows, err
}

func (s *chStore) AIMetric(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, windowSecs int64) (float64, bool, error) {
	if windowSecs <= 0 {
		windowSecs = 300
	}
	nowMs := time.Now().UnixMilli()
	startMs := nowMs - windowSecs*1000
	return s.aiMetricRange(ctx, teamID, targetRef, metric, startMs, nowMs)
}

func (s *chStore) AIMetricHistorical(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs
	return s.aiMetricRange(ctx, teamID, targetRef, metric, fromMs, toMs)
}

// aiMetricRange serves AI metrics from `ai_spans_rollup_v2_*` when the target
// filter is expressible in rollup keys (service / provider / model). Falls
// back to raw when the filter uses `prompt_template` (not rollup-keyed).
func (s *chStore) aiMetricRange(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, startMs, endMs int64) (float64, bool, error) {
	if where, args, ok := shared.BuildAIRollupWhereFromRef(teamID, targetRef); ok {
		return s.queryAIMetricRollup(ctx, metric, where, args, startMs, endMs)
	}
	where, args := shared.BuildAIWhereFromRef(teamID, targetRef)
	where += " AND s.timestamp BETWEEN @start AND @end"
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return s.queryAIMetricRaw(ctx, metric, where, args)
}

func (s *chStore) queryAIMetricRollup(ctx context.Context, metric, where string, args []any, startMs, endMs int64) (float64, bool, error) {
	var selectExpr string
	switch metric {
	case "error_rate_pct":
		selectExpr = "toFloat64(sumMerge(error_count)) * 100.0 / nullIf(toFloat64(sumMerge(request_count)), 0)"
	case "cost_usd":
		selectExpr = "sumMerge(cost_usd_sum)"
	case "latency_ms":
		selectExpr = "quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1"
	case "quality_score":
		selectExpr = "sumMerge(quality_score_sum) / nullIf(toFloat64(sumMerge(quality_score_count)), 0)"
	default:
		return 0, false, fmt.Errorf("unsupported AI metric: %q", metric)
	}
	table, _ := rollup.TierTableFor(aiSpansRollupV2Prefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toFloat64(%s) AS val, toFloat64(sumMerge(request_count)) AS total
		FROM %s
		WHERE %s AND bucket_ts BETWEEN @start AND @end`, selectExpr, table, where)
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	var row struct {
		Val   float64 `ch:"val"`
		Total float64 `ch:"total"`
	}
	if err := s.ch.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil
	}
	return row.Val, false, nil
}

// queryAIMetricRaw is the fallback for rules that target prompt_template —
// a dim the rollup doesn't key. Scans raw spans with the full WHERE builder.
func (s *chStore) queryAIMetricRaw(ctx context.Context, metric, where string, args []any) (float64, bool, error) {
	var selectExpr string
	switch metric {
	case "error_rate_pct":
		selectExpr = "countIf(s.has_error) * 100.0 / count()"
	case "cost_usd":
		selectExpr = "sum(toFloat64OrZero(mapGet(s.attributes, 'optikk.ai.cost_usd')))"
	case "latency_ms":
		selectExpr = "avg(s.duration_nano / 1000000.0)"
	case "quality_score":
		selectExpr = "if(countIf(toFloat64OrZero(mapGet(s.attributes, 'optikk.ai.quality.score')) > 0) = 0, 0, avgIf(toFloat64OrZero(mapGet(s.attributes, 'optikk.ai.quality.score')), toFloat64OrZero(mapGet(s.attributes, 'optikk.ai.quality.score')) > 0))"
	default:
		return 0, false, fmt.Errorf("unsupported AI metric: %q", metric)
	}
	query := fmt.Sprintf(`
		SELECT toFloat64(%s) AS val, count() AS total
		FROM observability.spans s
		WHERE %s`, selectExpr, where)
	var row struct {
		Val   float64 `ch:"val"`
		Total uint64  `ch:"total"`
	}
	if err := s.ch.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return 0, false, err
	}
	if row.Total == 0 {
		return 0, true, nil
	}
	return row.Val, false, nil
}
