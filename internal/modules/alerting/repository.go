package alerting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Repository owns all storage interactions for alerting:
//   - MySQL `observability.alerts` for rule + instance state (single table per plan)
//   - ClickHouse `observability.alert_events` for the append-only audit / timeline
//   - ClickHouse observability.spans for the active evaluators (slo/error_rate)
//
// All SQL lives in this file per the 6-file module rule.
type Repository interface {
	// MySQL — rules CRUD / state
	CreateRule(ctx context.Context, r *Rule) (int64, error)
	UpdateRule(ctx context.Context, r *Rule) error
	DeleteRule(ctx context.Context, teamID, id int64) error
	GetRule(ctx context.Context, teamID, id int64) (*Rule, error)
	GetRuleInternal(ctx context.Context, id int64) (*Rule, error)
	ListRules(ctx context.Context, teamID int64) ([]*Rule, error)
	ListEnabledRules(ctx context.Context) ([]*Rule, error)
	SaveRuleRuntime(ctx context.Context, r *Rule) error

	// ClickHouse — events / audit
	WriteEvent(ctx context.Context, ev AlertEvent) error
	ListEvents(ctx context.Context, teamID, alertID int64, limit int) ([]AlertEvent, error)

	// Evaluator data queries
	SLOErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ServiceErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error)
	AIMetric(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, windowSecs int64) (float64, bool, error)
	AIMetricHistorical(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, fromMs, toMs, windowSecs int64) (float64, bool, error)

	// Deploy correlation — reused from deployments domain conceptually, but
	// implemented inline to keep the import graph flat.
	DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]DeployRef, error)
}

// DeployRef is a minimal deploy hit for correlation payloads.
type DeployRef struct {
	ServiceName string    `json:"service_name" ch:"service_name"`
	Version     string    `json:"version" ch:"version"`
	Environment string    `json:"environment" ch:"environment"`
	FirstSeen   time.Time `json:"first_seen" ch:"first_seen"`
}

type repository struct {
	db     *sql.DB
	ch     *dbutil.NativeQuerier
	chConn clickhouse.Conn
}

func NewRepository(db *sql.DB, ch *dbutil.NativeQuerier, chConn clickhouse.Conn) Repository {
	return &repository{db: db, ch: ch, chConn: chConn}
}

// ------------------- MySQL: rules -------------------

const alertColumns = `id, team_id, name, description, condition_type, target_ref, group_by, windows,
	operator, warn_threshold, critical_threshold, recovery_threshold,
	for_secs, recover_for_secs, keep_alive_secs, no_data_secs,
	severity, notify_template, max_notifs_per_hour, slack_webhook_url,
	rule_state, last_eval_at, instances, mute_until, silences,
	enabled, parent_alert_id, created_at, updated_at, created_by, updated_by`

func (r *repository) CreateRule(ctx context.Context, rule *Rule) (int64, error) {
	groupByJSON, _ := json.Marshal(rule.GroupBy)
	windowsJSON, _ := json.Marshal(rule.Windows)
	instancesJSON, _ := json.Marshal(rule.Instances)
	silencesJSON, _ := json.Marshal(rule.Silences)
	now := time.Now().UTC()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}
	rule.UpdatedAt = now

	res, err := r.db.ExecContext(ctx, `
		INSERT INTO observability.alerts
		(team_id, name, description, condition_type, target_ref, group_by, windows,
		 operator, warn_threshold, critical_threshold, recovery_threshold,
		 for_secs, recover_for_secs, keep_alive_secs, no_data_secs,
		 severity, notify_template, max_notifs_per_hour, slack_webhook_url,
		 rule_state, last_eval_at, instances, mute_until, silences,
		 enabled, parent_alert_id, created_at, updated_at, created_by, updated_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rule.TeamID, rule.Name, rule.Description, rule.ConditionType,
		string(ensureJSONObject(rule.TargetRef)), string(groupByJSON), string(windowsJSON),
		rule.Operator, rule.WarnThreshold, rule.CriticalThreshold, rule.RecoveryThreshold,
		rule.ForSecs, rule.RecoverForSecs, rule.KeepAliveSecs, rule.NoDataSecs,
		rule.Severity, rule.NotifyTemplate, rule.MaxNotifsPerHour, rule.SlackWebhookURL,
		emptyIfEmpty(rule.RuleState, StateOK), rule.LastEvalAt, string(instancesJSON),
		rule.MuteUntil, string(silencesJSON),
		rule.Enabled, rule.ParentID, rule.CreatedAt, rule.UpdatedAt, rule.CreatedBy, rule.UpdatedBy,
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	rule.ID = id
	return id, nil
}

func (r *repository) UpdateRule(ctx context.Context, rule *Rule) error {
	groupByJSON, _ := json.Marshal(rule.GroupBy)
	windowsJSON, _ := json.Marshal(rule.Windows)
	silencesJSON, _ := json.Marshal(rule.Silences)
	rule.UpdatedAt = time.Now().UTC()
	_, err := r.db.ExecContext(ctx, `
		UPDATE observability.alerts
		SET name=?, description=?, condition_type=?, target_ref=?, group_by=?, windows=?,
		    operator=?, warn_threshold=?, critical_threshold=?, recovery_threshold=?,
		    for_secs=?, recover_for_secs=?, keep_alive_secs=?, no_data_secs=?,
		    severity=?, notify_template=?, max_notifs_per_hour=?, slack_webhook_url=?,
		    mute_until=?, silences=?, enabled=?, updated_at=?, updated_by=?
		WHERE id=? AND team_id=?`,
		rule.Name, rule.Description, rule.ConditionType,
		string(ensureJSONObject(rule.TargetRef)), string(groupByJSON), string(windowsJSON),
		rule.Operator, rule.WarnThreshold, rule.CriticalThreshold, rule.RecoveryThreshold,
		rule.ForSecs, rule.RecoverForSecs, rule.KeepAliveSecs, rule.NoDataSecs,
		rule.Severity, rule.NotifyTemplate, rule.MaxNotifsPerHour, rule.SlackWebhookURL,
		rule.MuteUntil, string(silencesJSON),
		rule.Enabled, rule.UpdatedAt, rule.UpdatedBy,
		rule.ID, rule.TeamID,
	)
	return err
}

func (r *repository) SaveRuleRuntime(ctx context.Context, rule *Rule) error {
	instancesJSON, _ := json.Marshal(rule.Instances)
	_, err := r.db.ExecContext(ctx, `
		UPDATE observability.alerts
		SET rule_state=?, last_eval_at=?, instances=?
		WHERE id=?`,
		rule.RuleState, rule.LastEvalAt, string(instancesJSON), rule.ID)
	return err
}

func (r *repository) DeleteRule(ctx context.Context, teamID, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM observability.alerts WHERE id=? AND team_id=?`, id, teamID)
	return err
}

func (r *repository) GetRule(ctx context.Context, teamID, id int64) (*Rule, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE id=? AND team_id=?`, id, teamID)
	return scanRule(row.Scan)
}

func (r *repository) GetRuleInternal(ctx context.Context, id int64) (*Rule, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE id=?`, id)
	return scanRule(row.Scan)
}

func (r *repository) ListRules(ctx context.Context, teamID int64) ([]*Rule, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE team_id=? ORDER BY id DESC`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Rule
	for rows.Next() {
		rule, err := scanRule(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	return out, rows.Err()
}

func (r *repository) ListEnabledRules(ctx context.Context) ([]*Rule, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE enabled=1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Rule
	for rows.Next() {
		rule, err := scanRule(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	return out, rows.Err()
}

type scanFn func(dest ...any) error

func scanRule(scan scanFn) (*Rule, error) {
	var (
		rule              Rule
		targetRef         sql.NullString
		groupByJSON       sql.NullString
		windowsJSON       sql.NullString
		instancesJSON     sql.NullString
		silencesJSON      sql.NullString
		warnThreshold     sql.NullFloat64
		recoveryThreshold sql.NullFloat64
		description       sql.NullString
		lastEvalAt        sql.NullTime
		muteUntil         sql.NullTime
		notifyTemplate    sql.NullString
		slackWebhookURL   sql.NullString
		parentID          sql.NullInt64
		ruleState         sql.NullString
	)
	err := scan(
		&rule.ID, &rule.TeamID, &rule.Name, &description, &rule.ConditionType,
		&targetRef, &groupByJSON, &windowsJSON,
		&rule.Operator, &warnThreshold, &rule.CriticalThreshold, &recoveryThreshold,
		&rule.ForSecs, &rule.RecoverForSecs, &rule.KeepAliveSecs, &rule.NoDataSecs,
		&rule.Severity, &notifyTemplate, &rule.MaxNotifsPerHour, &slackWebhookURL,
		&ruleState, &lastEvalAt, &instancesJSON, &muteUntil, &silencesJSON,
		&rule.Enabled, &parentID, &rule.CreatedAt, &rule.UpdatedAt, &rule.CreatedBy, &rule.UpdatedBy,
	)
	if err != nil {
		return nil, err
	}
	rule.Description = description.String
	if targetRef.Valid && targetRef.String != "" {
		rule.TargetRef = json.RawMessage(targetRef.String)
	}
	if groupByJSON.Valid && groupByJSON.String != "" {
		_ = json.Unmarshal([]byte(groupByJSON.String), &rule.GroupBy)
	}
	if windowsJSON.Valid && windowsJSON.String != "" {
		_ = json.Unmarshal([]byte(windowsJSON.String), &rule.Windows)
	}
	if instancesJSON.Valid && instancesJSON.String != "" {
		_ = json.Unmarshal([]byte(instancesJSON.String), &rule.Instances)
	}
	if rule.Instances == nil {
		rule.Instances = InstancesMap{}
	}
	if silencesJSON.Valid && silencesJSON.String != "" {
		_ = json.Unmarshal([]byte(silencesJSON.String), &rule.Silences)
	}
	if warnThreshold.Valid {
		v := warnThreshold.Float64
		rule.WarnThreshold = &v
	}
	if recoveryThreshold.Valid {
		v := recoveryThreshold.Float64
		rule.RecoveryThreshold = &v
	}
	if lastEvalAt.Valid {
		t := lastEvalAt.Time
		rule.LastEvalAt = &t
	}
	if muteUntil.Valid {
		t := muteUntil.Time
		rule.MuteUntil = &t
	}
	rule.NotifyTemplate = notifyTemplate.String
	rule.SlackWebhookURL = slackWebhookURL.String
	if parentID.Valid {
		v := parentID.Int64
		rule.ParentID = &v
	}
	if ruleState.Valid {
		rule.RuleState = ruleState.String
	}
	if rule.RuleState == "" {
		rule.RuleState = StateOK
	}
	return &rule, nil
}

// ------------------- ClickHouse: events -------------------

func (r *repository) WriteEvent(ctx context.Context, ev AlertEvent) error {
	if r.chConn == nil {
		return nil
	}
	if ev.Ts.IsZero() {
		ev.Ts = time.Now().UTC()
	}
	return r.chConn.Exec(ctx, `
		INSERT INTO observability.alert_events
		(ts, team_id, alert_id, instance_key, kind, from_state, to_state, values, actor_user_id, message, deploy_refs, transition_id)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		ev.Ts, ev.TeamID, ev.AlertID, ev.InstanceKey, ev.Kind, ev.FromState, ev.ToState,
		ev.Values, ev.ActorUserID, ev.Message, ev.DeployRefs, ev.TransitionID,
	)
}

func (r *repository) ListEvents(ctx context.Context, teamID, alertID int64, limit int) ([]AlertEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	var rows []AlertEvent
	err := r.ch.Select(ctx, &rows, `
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

// ------------------- Evaluator queries -------------------

// SLOErrorRate returns the current error rate (%) over the last windowSecs for the
// given service (or "" for team-wide), matching the SLO definition used by
// overview/slo (root spans + has_error OR 5xx).
func (r *repository) SLOErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	return r.errorRateLast(ctx, teamID, serviceName, windowSecs)
}

// ServiceErrorRate matches overview/errors semantics (any erroring span, not just
// root). In v1 we use the same root-span definition used by the SLO burn-rate so
// the two evaluators stay consistent with dashboards the user is looking at.
func (r *repository) ServiceErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
	return r.errorRateLast(ctx, teamID, serviceName, windowSecs)
}

func (r *repository) errorRateLast(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error) {
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
	if err := r.ch.QueryRow(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil // NoData
	}
	return row.Errs * 100.0 / row.Total, false, nil
}

// ErrorRateHistorical is the backtest variant: error rate for a closed [from,to] window.
func (r *repository) ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
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
	if err := r.ch.QueryRow(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total <= 0 {
		return 0, true, nil
	}
	return row.Errs * 100.0 / row.Total, false, nil
}

// DeploysInRange returns root-span deploy markers in [from,to] — used for
// correlation payloads on transitions.
func (r *repository) DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]DeployRef, error) {
	var rows []DeployRef
	err := r.ch.Select(ctx, &rows, `
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

// ------------------- AI Metric Queries -------------------

func (r *repository) AIMetric(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, windowSecs int64) (float64, bool, error) {
	if windowSecs <= 0 {
		windowSecs = 300
	}
	where, args := buildAIWhereFromRef(teamID, targetRef)
	where += fmt.Sprintf(" AND s.timestamp >= now() - INTERVAL %d SECOND", windowSecs)

	return r.queryAIMetric(ctx, metric, where, args)
}

func (r *repository) AIMetricHistorical(ctx context.Context, teamID int64, targetRef json.RawMessage, metric string, fromMs, toMs, windowSecs int64) (float64, bool, error) {
	_ = windowSecs // not used for exact range historical queries
	where, args := buildAIWhereFromRef(teamID, targetRef)
	where += " AND s.timestamp BETWEEN @start AND @end"
	args = append(args,
		clickhouse.Named("start", time.UnixMilli(fromMs)),
		clickhouse.Named("end", time.UnixMilli(toMs)),
	)

	return r.queryAIMetric(ctx, metric, where, args)
}

func (r *repository) queryAIMetric(ctx context.Context, metric, where string, args []any) (float64, bool, error) {
	var selectExpr string
	switch metric {
	case "error_rate_pct":
		selectExpr = "countIf(s.has_error) * 100.0 / count()"
	case "cost_usd":
		selectExpr = "sum(toFloat64OrZero(mapGet(s.attributes, 'optikk.ai.cost_usd')))"
	case "latency_ms":
		selectExpr = "avg(s.duration_nano / 1000000.0)"
	case "quality_score":
		// Only average scored runs
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
	if err := r.ch.QueryRow(ctx, &row, query, args...); err != nil {
		return 0, false, err
	}
	if row.Total == 0 {
		return 0, true, nil
	}
	return row.Val, false, nil
}

func buildAIWhereFromRef(teamID int64, targetRef json.RawMessage) (string, []any) {
	where := "s.team_id = @teamID"
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec

	if len(targetRef) == 0 {
		return where, args
	}

	var m map[string]any
	if err := json.Unmarshal(targetRef, &m); err != nil {
		return where, args
	}

	// Service filter
	for _, k := range []string{"service_name", "service", "serviceName"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND s.service_name = @serviceName"
			args = append(args, clickhouse.Named("serviceName", v))
			break
		}
	}

	// Provider filter
	if v, ok := m["provider"].(string); ok && v != "" {
		where += " AND (mapGet(s.attributes, 'optikk.ai.provider') = @provider OR mapGet(s.attributes, 'llm.provider') = @provider OR mapGet(s.attributes, 'gen_ai.system') = @provider)"
		args = append(args, clickhouse.Named("provider", v))
	}

	// Model filter
	for _, k := range []string{"model", "request_model", "requestModel"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND (mapGet(s.attributes, 'optikk.ai.model') = @model OR mapGet(s.attributes, 'llm.request.model') = @model OR mapGet(s.attributes, 'gen_ai.request.model') = @model)"
			args = append(args, clickhouse.Named("model", v))
			break
		}
	}

	// Prompt template filter
	for _, k := range []string{"prompt_template", "promptTemplate"} {
		if v, ok := m[k].(string); ok && v != "" {
			where += " AND (mapGet(s.attributes, 'optikk.ai.prompt_template.name') = @promptTemplate OR mapGet(s.attributes, 'gen_ai.prompt.template.name') = @promptTemplate)"
			args = append(args, clickhouse.Named("promptTemplate", v))
			break
		}
	}

	return where, args
}

func ensureJSONObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func emptyIfEmpty(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
