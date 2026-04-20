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
	"golang.org/x/sync/errgroup"
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

// errorRateLast runs two parallel narrow-WHERE scans (totals + errors) and
// divides in Go, avoiding countIf combinators and toUInt16OrZero casts. The
// http_status_code alias column on observability.spans is already UInt16 so
// the >= 500 comparison is a plain numeric predicate.
func (s *chStore) errorRateLast(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	if windowSecs <= 0 {
		windowSecs = 300
	}
	where := fmt.Sprintf(`s.team_id = @teamID
	              AND s.timestamp >= now() - INTERVAL %d SECOND
	              AND s.parent_span_id = ''`, windowSecs)
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec
	if strings.TrimSpace(serviceName) != "" {
		where += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	return s.runErrorRateSplit(ctx, dbutil.OverviewCtx(ctx), where, args)
}

func (s *chStore) ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs
	where := `s.team_id = @teamID
	          AND s.timestamp BETWEEN @start AND @end
	          AND s.parent_span_id = ''`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(fromMs)),
		clickhouse.Named("end", time.UnixMilli(toMs)),
	}
	if strings.TrimSpace(serviceName) != "" {
		where += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	return s.runErrorRateSplit(ctx, dbutil.ExplorerCtx(ctx), where, args)
}

// runErrorRateSplit executes the totals + errors scans in parallel and
// returns percent errors. `queryCtx` carries the budget (Overview or
// Explorer); each leg uses that same ctx so both queries share the budget.
func (s *chStore) runErrorRateSplit(ctx context.Context, queryCtx context.Context, where string, args []any) (float64, bool, error) {
	_ = ctx
	type countRow struct {
		C uint64 `ch:"c"`
	}
	var (
		total countRow
		errs  countRow
	)
	g, gctx := errgroup.WithContext(queryCtx)
	g.Go(func() error {
		q := `SELECT count() AS c FROM observability.spans s WHERE ` + where
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&total)
	})
	g.Go(func() error {
		q := `SELECT count() AS c FROM observability.spans s WHERE ` + where +
			` AND (s.has_error = true OR s.http_status_code >= 500)`
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&errs)
	})
	if err := g.Wait(); err != nil {
		return 0, false, err
	}
	if total.C == 0 {
		return 0, true, nil
	}
	return float64(errs.C) * 100.0 / float64(total.C), false, nil
}

func (s *chStore) DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]shared.DeployRef, error) {
	var rows []shared.DeployRef
	err := s.ch.Select(dbutil.OverviewCtx(ctx), &rows, `
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
	switch metric {
	case "error_rate_pct":
		return s.aiErrorRate(ctx, where, args)
	case "cost_usd":
		return s.aiCostSum(ctx, where, args)
	case "latency_ms":
		return s.aiLatencyAvg(ctx, where, args)
	case "quality_score":
		return s.aiQualityAvg(ctx, where, args)
	default:
		return 0, false, fmt.Errorf("unsupported AI metric: %q", metric)
	}
}

// aiErrorRate runs a totals + errors split (mirror of runErrorRateSplit but
// with an AI-specific has_error-only predicate) so SELECT stays combinator-free.
func (s *chStore) aiErrorRate(ctx context.Context, where string, args []any) (float64, bool, error) {
	type countRow struct {
		C uint64 `ch:"c"`
	}
	var (
		total countRow
		errs  countRow
	)
	g, gctx := errgroup.WithContext(dbutil.OverviewCtx(ctx))
	g.Go(func() error {
		q := `SELECT count() AS c FROM observability.spans s WHERE ` + where
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&total)
	})
	g.Go(func() error {
		q := `SELECT count() AS c FROM observability.spans s WHERE ` + where +
			` AND s.has_error = true`
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&errs)
	})
	if err := g.Wait(); err != nil {
		return 0, false, err
	}
	if total.C == 0 {
		return 0, true, nil
	}
	return float64(errs.C) * 100.0 / float64(total.C), false, nil
}

// aiCostSum returns sum(optikk.ai.cost_usd) with total for no-data detection.
// The CAST-to-Float64 operator (::Float64) is permitted; it replaces the
// toFloat64OrZero() wrapper on the raw attribute string.
func (s *chStore) aiCostSum(ctx context.Context, where string, args []any) (float64, bool, error) {
	q := `SELECT sum(mapGet(s.attributes, 'optikk.ai.cost_usd')::Float64) AS val,
	             count() AS total
	      FROM observability.spans s
	      WHERE ` + where
	var row struct {
		Val   float64 `ch:"val"`
		Total uint64  `ch:"total"`
	}
	if err := s.ch.QueryRow(dbutil.OverviewCtx(ctx), q, args...).ScanStruct(&row); err != nil {
		return 0, false, err
	}
	if row.Total == 0 {
		return 0, true, nil
	}
	return row.Val, false, nil
}

// aiLatencyAvg returns avg-latency (ms) computed Go-side from sum/count; no
// SQL `avg(col)` is emitted.
func (s *chStore) aiLatencyAvg(ctx context.Context, where string, args []any) (float64, bool, error) {
	q := `SELECT sum(s.duration_nano / 1000000.0) AS val_sum,
	             count() AS total
	      FROM observability.spans s
	      WHERE ` + where
	var row struct {
		ValSum float64 `ch:"val_sum"`
		Total  uint64  `ch:"total"`
	}
	if err := s.ch.QueryRow(dbutil.OverviewCtx(ctx), q, args...).ScanStruct(&row); err != nil {
		return 0, false, err
	}
	if row.Total == 0 {
		return 0, true, nil
	}
	return row.ValSum / float64(row.Total), false, nil
}

// aiQualityAvg returns avg(quality) over rows where the score is > 0. Runs
// two narrow scans (total + positive-score sum/count) and divides in Go so
// SELECT stays free of avgIf/if/countIf.
func (s *chStore) aiQualityAvg(ctx context.Context, where string, args []any) (float64, bool, error) {
	type totalRow struct {
		C uint64 `ch:"c"`
	}
	type scoreRow struct {
		Sum float64 `ch:"s"`
		Cnt uint64  `ch:"c"`
	}
	var (
		total totalRow
		score scoreRow
	)
	g, gctx := errgroup.WithContext(dbutil.OverviewCtx(ctx))
	g.Go(func() error {
		q := `SELECT count() AS c FROM observability.spans s WHERE ` + where
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&total)
	})
	g.Go(func() error {
		q := `SELECT sum(mapGet(s.attributes, 'optikk.ai.quality.score')::Float64) AS s,
		             count() AS c
		      FROM observability.spans s
		      WHERE ` + where +
			` AND mapGet(s.attributes, 'optikk.ai.quality.score')::Float64 > 0`
		return s.ch.QueryRow(gctx, q, args...).ScanStruct(&score)
	})
	if err := g.Wait(); err != nil {
		return 0, false, err
	}
	if total.C == 0 {
		return 0, true, nil
	}
	if score.Cnt == 0 {
		return 0, false, nil
	}
	return score.Sum / float64(score.Cnt), false, nil
}
