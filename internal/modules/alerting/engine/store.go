package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
)

// EventStore persists alerting audit events (ClickHouse observability.alert_events).
// Writes go through the native driver (Exec), reads through the NativeQuerier.
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
	ch     *dbutil.NativeQuerier
	chConn clickhouse.Conn
}

// NewStore constructs the shared ClickHouse-backed event store + data source.
// Callers can type-assert the returned value to EventStore or DataSource.
func NewStore(ch *dbutil.NativeQuerier, chConn clickhouse.Conn) *chStore { //nolint:revive // ok
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
	err := s.ch.SelectOverview(ctx, &rows, `
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
	err := s.ch.SelectOverview(ctx, &rows, `
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
	query := fmt.Sprintf(`
		SELECT toFloat64(count()) AS total,
		       toFloat64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 500)) AS errs
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.timestamp >= now() - INTERVAL %d SECOND
		  AND s.parent_span_id = ''`, windowSecs)
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec
	if strings.TrimSpace(serviceName) != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	var row struct {
		Total float64 `ch:"total"`
		Errs  float64 `ch:"errs"`
	}
	if err := s.ch.QueryRowOverview(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil
	}
	return row.Errs * 100.0 / row.Total, false, nil
}

func (s *chStore) ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs
	query := `
		SELECT toFloat64(count()) AS total,
		       toFloat64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 500)) AS errs
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.parent_span_id = ''`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(fromMs)),
		clickhouse.Named("end", time.UnixMilli(toMs)),
	}
	if strings.TrimSpace(serviceName) != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	var row struct {
		Total float64 `ch:"total"`
		Errs  float64 `ch:"errs"`
	}
	if err := s.ch.QueryRowExplorer(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil
	}
	return row.Errs * 100.0 / row.Total, false, nil
}

func (s *chStore) DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]shared.DeployRef, error) {
	var rows []shared.DeployRef
	err := s.ch.SelectOverview(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.timestamp BETWEEN @start AND @end
		  AND s.parent_span_id = ''
		  AND s.mat_service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC`,
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
	where, args := shared.BuildAIWhereFromRef(teamID, targetRef)
	where += fmt.Sprintf(" AND s.timestamp >= now() - INTERVAL %d SECOND", windowSecs)
	return s.queryAIMetric(ctx, metric, where, args)
}

func (s *chStore) AIMetricHistorical(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs
	where, args := shared.BuildAIWhereFromRef(teamID, targetRef)
	where += " AND s.timestamp BETWEEN @start AND @end"
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(fromMs)),
		clickhouse.Named("end", time.UnixMilli(toMs)),
	)
	return s.queryAIMetric(ctx, metric, where, args)
}

func (s *chStore) queryAIMetric(ctx context.Context, metric, where string, args []any) (float64, bool, error) {
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
	if err := s.ch.QueryRowOverview(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total == 0 {
		return 0, true, nil
	}
	return row.Val, false, nil
}
