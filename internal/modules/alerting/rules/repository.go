package rules

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
)

// Repository owns MySQL persistence for alert rules + inline instance / silence
// state (`observability.alerts`). Event audit reads (CH `observability.alert_events`)
// are served by engine.EventStore — this repository stays MySQL-only.
type Repository interface {
	Create(ctx context.Context, r *shared.Rule) (int64, error)
	Update(ctx context.Context, r *shared.Rule) error
	Delete(ctx context.Context, teamID, id int64) error
	Get(ctx context.Context, teamID, id int64) (*shared.Rule, error)
	GetInternal(ctx context.Context, id int64) (*shared.Rule, error)
	List(ctx context.Context, teamID int64) ([]*shared.Rule, error)
	ListEnabled(ctx context.Context) ([]*shared.Rule, error)
	SaveRuntime(ctx context.Context, r *shared.Rule) error

	// ListEnabledRules + SaveRuleRuntime mirror the engine.RuleStore contract
	// (longer names suit the "engine" reader, shorter ones suit the CRUD
	// service). Impl forwards to the shorter ones.
	ListEnabledRules(ctx context.Context) ([]*shared.Rule, error)
	SaveRuleRuntime(ctx context.Context, r *shared.Rule) error
}

type repository struct {
	db              *sql.DB
	maxEnabledRules int
}

// NewRepository wires the MySQL-backed rule repository.
func NewRepository(db *sql.DB, maxEnabledRules int) Repository {
	if maxEnabledRules <= 0 {
		maxEnabledRules = 10000
	}
	return &repository{db: db, maxEnabledRules: maxEnabledRules}
}

const alertColumns = `id, team_id, name, description, condition_type, target_ref, group_by, windows,
	operator, warn_threshold, critical_threshold, recovery_threshold,
	for_secs, recover_for_secs, keep_alive_secs, no_data_secs,
	severity, notify_template, max_notifs_per_hour, slack_webhook_url,
	rule_state, last_eval_at, instances, mute_until, silences,
	enabled, parent_alert_id, created_at, updated_at, created_by, updated_by`

func (r *repository) Create(ctx context.Context, rule *shared.Rule) (int64, error) {
	groupByJSON, err := json.Marshal(rule.GroupBy)
	if err != nil {
		return 0, fmt.Errorf("alerting: marshal group_by: %w", err)
	}
	windowsJSON, err := json.Marshal(rule.Windows)
	if err != nil {
		return 0, fmt.Errorf("alerting: marshal windows: %w", err)
	}
	instancesJSON, err := json.Marshal(rule.Instances)
	if err != nil {
		return 0, fmt.Errorf("alerting: marshal instances: %w", err)
	}
	silencesJSON, err := json.Marshal(rule.Silences)
	if err != nil {
		return 0, fmt.Errorf("alerting: marshal silences: %w", err)
	}
	now := time.Now().UTC()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}
	rule.UpdatedAt = now

	ruleState := rule.RuleState
	if ruleState == "" {
		ruleState = shared.StateOK
	}

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
		ruleState, rule.LastEvalAt, string(instancesJSON),
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

func (r *repository) Update(ctx context.Context, rule *shared.Rule) error {
	groupByJSON, err := json.Marshal(rule.GroupBy)
	if err != nil {
		return fmt.Errorf("alerting: marshal group_by: %w", err)
	}
	windowsJSON, err := json.Marshal(rule.Windows)
	if err != nil {
		return fmt.Errorf("alerting: marshal windows: %w", err)
	}
	silencesJSON, err := json.Marshal(rule.Silences)
	if err != nil {
		return fmt.Errorf("alerting: marshal silences: %w", err)
	}
	rule.UpdatedAt = time.Now().UTC()
	_, err = r.db.ExecContext(ctx, `
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

func (r *repository) SaveRuntime(ctx context.Context, rule *shared.Rule) error {
	instancesJSON, err := json.Marshal(rule.Instances)
	if err != nil {
		return fmt.Errorf("alerting: marshal instances: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE observability.alerts
		SET rule_state=?, last_eval_at=?, instances=?
		WHERE id=?`,
		rule.RuleState, rule.LastEvalAt, string(instancesJSON), rule.ID)
	return err
}

func (r *repository) Delete(ctx context.Context, teamID, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM observability.alerts WHERE id=? AND team_id=?`, id, teamID)
	return err
}

func (r *repository) Get(ctx context.Context, teamID, id int64) (*shared.Rule, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE id=? AND team_id=?`, id, teamID)
	return scanRule(row.Scan)
}

func (r *repository) GetInternal(ctx context.Context, id int64) (*shared.Rule, error) {
	row := r.db.QueryRowContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE id=?`, id)
	return scanRule(row.Scan)
}

func (r *repository) List(ctx context.Context, teamID int64) ([]*shared.Rule, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE team_id=? ORDER BY id DESC`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // ro cursor
	var out []*shared.Rule
	for rows.Next() {
		rule, err := scanRule(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	return out, rows.Err()
}

func (r *repository) ListEnabled(ctx context.Context) ([]*shared.Rule, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+alertColumns+` FROM observability.alerts WHERE enabled=1 LIMIT ?`, r.maxEnabledRules)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // ro cursor
	var out []*shared.Rule
	for rows.Next() {
		rule, err := scanRule(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == r.maxEnabledRules {
		slog.Warn("alerting: enabled rules hit safety cap", slog.Int("cap", r.maxEnabledRules))
	}
	return out, nil
}

// ListEnabledRules satisfies engine.RuleStore so the evaluator loop can depend
// on this repository without reaching across packages for a narrower interface.
func (r *repository) ListEnabledRules(ctx context.Context) ([]*shared.Rule, error) {
	return r.ListEnabled(ctx)
}

// SaveRuleRuntime satisfies engine.RuleStore.
func (r *repository) SaveRuleRuntime(ctx context.Context, rule *shared.Rule) error {
	return r.SaveRuntime(ctx, rule)
}

type scanFn func(dest ...any) error

func scanRule(scan scanFn) (*shared.Rule, error) {
	var (
		rule              shared.Rule
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
		if err := json.Unmarshal([]byte(groupByJSON.String), &rule.GroupBy); err != nil {
			slog.Warn("alerting: corrupt JSON in rule column", slog.Int64("rule_id", rule.ID), slog.String("column", "group_by"), slog.Any("error", err))
		}
	}
	if windowsJSON.Valid && windowsJSON.String != "" {
		if err := json.Unmarshal([]byte(windowsJSON.String), &rule.Windows); err != nil {
			slog.Warn("alerting: corrupt JSON in rule column", slog.Int64("rule_id", rule.ID), slog.String("column", "windows"), slog.Any("error", err))
		}
	}
	if instancesJSON.Valid && instancesJSON.String != "" {
		if err := json.Unmarshal([]byte(instancesJSON.String), &rule.Instances); err != nil {
			slog.Warn("alerting: corrupt JSON in rule column", slog.Int64("rule_id", rule.ID), slog.String("column", "instances"), slog.Any("error", err))
		}
	}
	if rule.Instances == nil {
		rule.Instances = shared.InstancesMap{}
	}
	if silencesJSON.Valid && silencesJSON.String != "" {
		if err := json.Unmarshal([]byte(silencesJSON.String), &rule.Silences); err != nil {
			slog.Warn("alerting: corrupt JSON in rule column", slog.Int64("rule_id", rule.ID), slog.String("column", "silences"), slog.Any("error", err))
		}
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
		rule.RuleState = shared.StateOK
	}
	return &rule, nil
}

func ensureJSONObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
