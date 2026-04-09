package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/google/uuid"
)

var variablePattern = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_]+)\s*\}\}`)

type Repository struct {
	db      *sql.DB
	querier dbutil.Querier
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		db:      db,
		querier: dbutil.NewMySQLWrapper(db),
	}
}

func (r *Repository) ListPrompts(ctx context.Context, teamID int64) ([]Prompt, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			p.id,
			p.team_id,
			p.name,
			p.slug,
			COALESCE(p.description, '') AS description,
			p.model_provider,
			p.model_name,
			COALESCE(v.system_prompt, '') AS system_prompt,
			COALESCE(v.user_template, '') AS user_template,
			COALESCE(p.tags_json, '[]') AS tags_json,
			p.latest_version,
			COALESCE(p.active_version_id, '') AS active_version_id,
			p.updated_at,
			p.created_at
		FROM ai_prompts p
		LEFT JOIN ai_prompt_versions v ON v.id = p.active_version_id
		WHERE p.team_id = ?
		ORDER BY p.updated_at DESC, p.created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return mapPromptRows(rows), nil
}

func (r *Repository) GetPrompt(ctx context.Context, teamID int64, promptID string) (Prompt, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			p.id,
			p.team_id,
			p.name,
			p.slug,
			COALESCE(p.description, '') AS description,
			p.model_provider,
			p.model_name,
			COALESCE(v.system_prompt, '') AS system_prompt,
			COALESCE(v.user_template, '') AS user_template,
			COALESCE(p.tags_json, '[]') AS tags_json,
			p.latest_version,
			COALESCE(p.active_version_id, '') AS active_version_id,
			p.updated_at,
			p.created_at
		FROM ai_prompts p
		LEFT JOIN ai_prompt_versions v ON v.id = p.active_version_id
		WHERE p.team_id = ? AND p.id = ?
		LIMIT 1
	`, teamID, promptID)
	if err != nil {
		return Prompt{}, err
	}
	if len(row) == 0 {
		return Prompt{}, sql.ErrNoRows
	}
	return promptFromRow(row), nil
}

func (r *Repository) CreatePrompt(ctx context.Context, teamID int64, input CreatePromptInput) (Prompt, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Prompt{}, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	promptID := uuid.NewString()
	versionID := uuid.NewString()
	variables := extractVariables(input.UserTemplate)

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ai_prompts (
			id, team_id, name, slug, description, model_provider, model_name,
			tags_json, latest_version, active_version_id, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, promptID, teamID, strings.TrimSpace(input.Name), strings.TrimSpace(input.Slug),
		nullableString(strings.TrimSpace(input.Description)),
		strings.TrimSpace(input.ModelProvider),
		strings.TrimSpace(input.ModelName),
		dbutil.JSONString(input.Tags),
		1,
		versionID,
		now,
		now,
	); err != nil {
		return Prompt{}, err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ai_prompt_versions (
			id, prompt_id, version_number, changelog, system_prompt, user_template,
			variables_json, is_active, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, versionID, promptID, 1, "Initial version", input.SystemPrompt, input.UserTemplate,
		dbutil.JSONString(variables), 1, now,
	); err != nil {
		return Prompt{}, err
	}

	if err := tx.Commit(); err != nil {
		return Prompt{}, err
	}

	return r.GetPrompt(ctx, teamID, promptID)
}

func (r *Repository) ListPromptVersions(ctx context.Context, teamID int64, promptID string) ([]PromptVersion, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			v.id,
			v.prompt_id,
			v.version_number,
			COALESCE(v.changelog, '') AS changelog,
			v.system_prompt,
			v.user_template,
			COALESCE(v.variables_json, '[]') AS variables_json,
			v.is_active,
			v.created_at
		FROM ai_prompt_versions v
		INNER JOIN ai_prompts p ON p.id = v.prompt_id
		WHERE p.team_id = ? AND v.prompt_id = ?
		ORDER BY v.version_number DESC
	`, teamID, promptID)
	if err != nil {
		return nil, err
	}
	return mapPromptVersionRows(rows), nil
}

func (r *Repository) GetPromptVersion(ctx context.Context, promptVersionID string) (PromptVersion, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			prompt_id,
			version_number,
			COALESCE(changelog, '') AS changelog,
			system_prompt,
			user_template,
			COALESCE(variables_json, '[]') AS variables_json,
			is_active,
			created_at
		FROM ai_prompt_versions
		WHERE id = ?
		LIMIT 1
	`, promptVersionID)
	if err != nil {
		return PromptVersion{}, err
	}
	if len(row) == 0 {
		return PromptVersion{}, sql.ErrNoRows
	}
	return promptVersionFromRow(row), nil
}

func (r *Repository) CreatePromptVersion(
	ctx context.Context,
	teamID int64,
	promptID string,
	input CreatePromptVersionInput,
) (PromptVersion, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT latest_version
		FROM ai_prompts
		WHERE team_id = ? AND id = ?
		LIMIT 1
	`, teamID, promptID)
	if err != nil {
		return PromptVersion{}, err
	}
	if len(row) == 0 {
		return PromptVersion{}, sql.ErrNoRows
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return PromptVersion{}, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	versionID := uuid.NewString()
	versionNumber := int(asInt64(row["latest_version"])) + 1
	variables := input.Variables
	if len(variables) == 0 {
		variables = extractVariables(input.UserTemplate)
	}

	if input.Activate {
		if _, err := tx.ExecContext(ctx, `
			UPDATE ai_prompt_versions
			SET is_active = 0
			WHERE prompt_id = ?
		`, promptID); err != nil {
			return PromptVersion{}, err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ai_prompt_versions (
			id, prompt_id, version_number, changelog, system_prompt, user_template,
			variables_json, is_active, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, versionID, promptID, versionNumber, nullableString(input.Changelog), input.SystemPrompt,
		input.UserTemplate, dbutil.JSONString(variables), boolToTinyInt(input.Activate), now,
	); err != nil {
		return PromptVersion{}, err
	}

	activeVersionID := ""
	if input.Activate {
		activeVersionID = versionID
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE ai_prompts
		SET latest_version = ?, active_version_id = CASE WHEN ? <> '' THEN ? ELSE active_version_id END, updated_at = ?
		WHERE id = ? AND team_id = ?
	`, versionNumber, activeVersionID, activeVersionID, now, promptID, teamID); err != nil {
		return PromptVersion{}, err
	}

	if err := tx.Commit(); err != nil {
		return PromptVersion{}, err
	}

	return r.GetPromptVersion(ctx, versionID)
}

func (r *Repository) ListDatasets(ctx context.Context, teamID int64) ([]Dataset, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			COALESCE(tags_json, '[]') AS tags_json,
			item_count,
			updated_at,
			created_at
		FROM ai_datasets
		WHERE team_id = ?
		ORDER BY updated_at DESC, created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return mapDatasetRows(rows), nil
}

func (r *Repository) GetDataset(ctx context.Context, teamID int64, datasetID string) (Dataset, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			COALESCE(tags_json, '[]') AS tags_json,
			item_count,
			updated_at,
			created_at
		FROM ai_datasets
		WHERE team_id = ? AND id = ?
		LIMIT 1
	`, teamID, datasetID)
	if err != nil {
		return Dataset{}, err
	}
	if len(row) == 0 {
		return Dataset{}, sql.ErrNoRows
	}
	return datasetFromRow(row), nil
}

func (r *Repository) CreateDataset(ctx context.Context, teamID int64, input CreateDatasetInput) (Dataset, error) {
	id := uuid.NewString()
	now := time.Now().UTC()
	_, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_datasets (
			id, team_id, name, description, tags_json, item_count, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, id, teamID, strings.TrimSpace(input.Name), nullableString(strings.TrimSpace(input.Description)),
		dbutil.JSONString(input.Tags), 0, now, now,
	)
	if err != nil {
		return Dataset{}, err
	}
	return r.GetDataset(ctx, teamID, id)
}

func (r *Repository) ListDatasetItems(ctx context.Context, teamID int64, datasetID string) ([]DatasetItem, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			i.id,
			i.dataset_id,
			COALESCE(i.input_json, '{}') AS input_json,
			COALESCE(i.expected_output, '') AS expected_output,
			COALESCE(i.metadata_json, '{}') AS metadata_json,
			i.created_at
		FROM ai_dataset_items i
		INNER JOIN ai_datasets d ON d.id = i.dataset_id
		WHERE d.team_id = ? AND i.dataset_id = ?
		ORDER BY i.created_at DESC
	`, teamID, datasetID)
	if err != nil {
		return nil, err
	}
	return mapDatasetItemRows(rows), nil
}

func (r *Repository) CreateDatasetItem(
	ctx context.Context,
	teamID int64,
	datasetID string,
	input CreateDatasetItemInput,
) (DatasetItem, error) {
	id := uuid.NewString()
	now := time.Now().UTC()

	res, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_dataset_items (
			id, dataset_id, input_json, expected_output, metadata_json, created_at
		)
		SELECT ?, d.id, ?, ?, ?, ?
		FROM ai_datasets d
		WHERE d.id = ? AND d.team_id = ?
	`, id, dbutil.JSONString(input.Input), input.ExpectedOutput, dbutil.JSONString(input.Metadata), now, datasetID, teamID)
	if err != nil {
		return DatasetItem{}, err
	}
	if dbutil.RowsAffected(res) == 0 {
		return DatasetItem{}, sql.ErrNoRows
	}

	if _, err := r.querier.ExecContext(ctx, `
		UPDATE ai_datasets
		SET item_count = item_count + 1, updated_at = ?
		WHERE id = ? AND team_id = ?
	`, now, datasetID, teamID); err != nil {
		return DatasetItem{}, err
	}

	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			dataset_id,
			COALESCE(input_json, '{}') AS input_json,
			COALESCE(expected_output, '') AS expected_output,
			COALESCE(metadata_json, '{}') AS metadata_json,
			created_at
		FROM ai_dataset_items
		WHERE id = ?
		LIMIT 1
	`, id)
	if err != nil {
		return DatasetItem{}, err
	}
	if len(rows) == 0 {
		return DatasetItem{}, sql.ErrNoRows
	}
	return datasetItemFromRow(rows[0]), nil
}

func (r *Repository) ListFeedback(
	ctx context.Context,
	teamID int64,
	targetType string,
	targetID string,
) ([]Feedback, error) {
	query := `
		SELECT
			id,
			team_id,
			target_type,
			target_id,
			COALESCE(run_span_id, '') AS run_span_id,
			COALESCE(trace_id, '') AS trace_id,
			score,
			label,
			COALESCE(comment, '') AS comment,
			COALESCE(created_by, 'system') AS created_by,
			created_at
		FROM ai_feedback
		WHERE team_id = ?
	`
	args := []any{teamID}
	if targetType != "" {
		query += " AND target_type = ?"
		args = append(args, targetType)
	}
	if targetID != "" {
		query += " AND target_id = ?"
		args = append(args, targetID)
	}
	query += " ORDER BY created_at DESC"

	rows, err := dbutil.QueryMaps(r.querier, query, args...)
	if err != nil {
		return nil, err
	}
	return mapFeedbackRows(rows), nil
}

func (r *Repository) CreateFeedback(
	ctx context.Context,
	teamID int64,
	createdBy string,
	input CreateFeedbackInput,
) (Feedback, error) {
	id := uuid.NewString()
	now := time.Now().UTC()
	_, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_feedback (
			id, team_id, target_type, target_id, run_span_id, trace_id,
			score, label, comment, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, teamID, input.TargetType, input.TargetID, nullableString(input.RunSpanID),
		nullableString(input.TraceID), input.Score, input.Label, nullableString(input.Comment),
		nullableString(createdBy), now,
	)
	if err != nil {
		return Feedback{}, err
	}

	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			target_type,
			target_id,
			COALESCE(run_span_id, '') AS run_span_id,
			COALESCE(trace_id, '') AS trace_id,
			score,
			label,
			COALESCE(comment, '') AS comment,
			COALESCE(created_by, 'system') AS created_by,
			created_at
		FROM ai_feedback
		WHERE id = ?
		LIMIT 1
	`, id)
	if err != nil {
		return Feedback{}, err
	}
	if len(rows) == 0 {
		return Feedback{}, sql.ErrNoRows
	}
	return feedbackFromRow(rows[0]), nil
}

func (r *Repository) ListEvals(ctx context.Context, teamID int64) ([]EvalSuite, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			prompt_id,
			dataset_id,
			judge_model,
			status,
			updated_at,
			created_at
		FROM ai_eval_suites
		WHERE team_id = ?
		ORDER BY updated_at DESC, created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return mapEvalRows(rows), nil
}

func (r *Repository) GetEval(ctx context.Context, teamID int64, evalID string) (EvalSuite, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			prompt_id,
			dataset_id,
			judge_model,
			status,
			updated_at,
			created_at
		FROM ai_eval_suites
		WHERE team_id = ? AND id = ?
		LIMIT 1
	`, teamID, evalID)
	if err != nil {
		return EvalSuite{}, err
	}
	if len(row) == 0 {
		return EvalSuite{}, sql.ErrNoRows
	}
	return evalFromRow(row), nil
}

func (r *Repository) CreateEval(ctx context.Context, teamID int64, input CreateEvalInput) (EvalSuite, error) {
	id := uuid.NewString()
	now := time.Now().UTC()
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = "draft"
	}
	_, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_eval_suites (
			id, team_id, name, description, prompt_id, dataset_id, judge_model,
			status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, teamID, strings.TrimSpace(input.Name), nullableString(strings.TrimSpace(input.Description)),
		input.PromptID, input.DatasetID, input.JudgeModel, status, now, now,
	)
	if err != nil {
		return EvalSuite{}, err
	}
	return r.GetEval(ctx, teamID, id)
}

func (r *Repository) ListEvalRuns(ctx context.Context, teamID int64, evalID string) ([]EvalRun, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			eval_id,
			prompt_version_id,
			dataset_id,
			status,
			average_score,
			pass_rate,
			total_cases,
			completed_cases,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_eval_runs
		WHERE team_id = ? AND eval_id = ?
		ORDER BY created_at DESC
	`, teamID, evalID)
	if err != nil {
		return nil, err
	}
	return mapEvalRunRows(rows), nil
}

func (r *Repository) GetEvalRun(ctx context.Context, runID string) (EvalRun, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			team_id,
			eval_id,
			prompt_version_id,
			dataset_id,
			status,
			average_score,
			pass_rate,
			total_cases,
			completed_cases,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_eval_runs
		WHERE id = ?
		LIMIT 1
	`, runID)
	if err != nil {
		return EvalRun{}, err
	}
	if len(row) == 0 {
		return EvalRun{}, sql.ErrNoRows
	}
	return evalRunFromRow(row), nil
}

func (r *Repository) QueueEvalRun(
	ctx context.Context,
	teamID int64,
	evalID string,
	promptVersionID string,
) (EvalRun, error) {
	suite, err := r.GetEval(ctx, teamID, evalID)
	if err != nil {
		return EvalRun{}, err
	}

	totalCases, err := r.datasetItemCount(ctx, suite.DatasetID)
	if err != nil {
		return EvalRun{}, err
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	_, err = r.querier.ExecContext(ctx, `
		INSERT INTO ai_eval_runs (
			id, team_id, eval_id, prompt_version_id, dataset_id, status,
			average_score, pass_rate, total_cases, completed_cases, summary_json,
			started_at, finished_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, teamID, evalID, promptVersionID, suite.DatasetID, "queued", 0, 0, totalCases, 0, "{}",
		nil, nil, now,
	)
	if err != nil {
		return EvalRun{}, err
	}
	return r.GetEvalRun(ctx, id)
}

func (r *Repository) ListEvalScores(
	ctx context.Context,
	teamID int64,
	evalID string,
	runID string,
) ([]EvalScore, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			s.id,
			s.eval_run_id,
			s.dataset_item_id,
			s.score,
			s.result_label,
			COALESCE(s.reason, '') AS reason,
			COALESCE(s.output_text, '') AS output_text,
			s.created_at
		FROM ai_eval_scores s
		INNER JOIN ai_eval_runs r ON r.id = s.eval_run_id
		WHERE r.team_id = ? AND r.eval_id = ? AND s.eval_run_id = ?
		ORDER BY s.created_at DESC
	`, teamID, evalID, runID)
	if err != nil {
		return nil, err
	}
	return mapEvalScoreRows(rows), nil
}

func (r *Repository) AddEvalScore(
	ctx context.Context,
	runID string,
	itemID string,
	score float64,
	resultLabel string,
	reason string,
	outputText string,
) error {
	_, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_eval_scores (
			id, eval_run_id, dataset_item_id, score, result_label, reason, output_text, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, uuid.NewString(), runID, itemID, score, resultLabel, nullableString(reason),
		nullableString(outputText), time.Now().UTC(),
	)
	return err
}

func (r *Repository) ListQueuedEvalRuns(ctx context.Context, limit int) ([]EvalRun, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			eval_id,
			prompt_version_id,
			dataset_id,
			status,
			average_score,
			pass_rate,
			total_cases,
			completed_cases,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_eval_runs
		WHERE status = 'queued'
		ORDER BY created_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	return mapEvalRunRows(rows), nil
}

func (r *Repository) MarkEvalRunRunning(ctx context.Context, runID string) (bool, error) {
	res, err := r.querier.ExecContext(ctx, `
		UPDATE ai_eval_runs
		SET status = 'running', started_at = ?
		WHERE id = ? AND status = 'queued'
	`, time.Now().UTC(), runID)
	if err != nil {
		return false, err
	}
	return dbutil.RowsAffected(res) > 0, nil
}

func (r *Repository) CompleteEvalRun(
	ctx context.Context,
	runID string,
	averageScore float64,
	passRate float64,
	completedCases int,
	summary map[string]any,
) error {
	_, err := r.querier.ExecContext(ctx, `
		UPDATE ai_eval_runs
		SET status = 'completed',
			average_score = ?,
			pass_rate = ?,
			completed_cases = ?,
			summary_json = ?,
			finished_at = ?
		WHERE id = ?
	`, averageScore, passRate, completedCases, dbutil.JSONString(summary), time.Now().UTC(), runID)
	return err
}

func (r *Repository) FailEvalRun(ctx context.Context, runID string, message string) error {
	_, err := r.querier.ExecContext(ctx, `
		UPDATE ai_eval_runs
		SET status = 'failed',
			summary_json = ?,
			finished_at = ?
		WHERE id = ?
	`, dbutil.JSONString(map[string]any{"error": message}), time.Now().UTC(), runID)
	return err
}

func (r *Repository) ListExperiments(ctx context.Context, teamID int64) ([]Experiment, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			dataset_id,
			status,
			updated_at,
			created_at
		FROM ai_experiments
		WHERE team_id = ?
		ORDER BY updated_at DESC, created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return mapExperimentRows(rows), nil
}

func (r *Repository) GetExperiment(ctx context.Context, teamID int64, experimentID string) (Experiment, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			team_id,
			name,
			COALESCE(description, '') AS description,
			dataset_id,
			status,
			updated_at,
			created_at
		FROM ai_experiments
		WHERE team_id = ? AND id = ?
		LIMIT 1
	`, teamID, experimentID)
	if err != nil {
		return Experiment{}, err
	}
	if len(row) == 0 {
		return Experiment{}, sql.ErrNoRows
	}
	return experimentFromRow(row), nil
}

func (r *Repository) CreateExperiment(
	ctx context.Context,
	teamID int64,
	input CreateExperimentInput,
) (Experiment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Experiment{}, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	experimentID := uuid.NewString()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ai_experiments (
			id, team_id, name, description, dataset_id, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, experimentID, teamID, strings.TrimSpace(input.Name),
		nullableString(strings.TrimSpace(input.Description)),
		input.DatasetID, "draft", now, now,
	); err != nil {
		return Experiment{}, err
	}

	for _, variant := range input.Variants {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ai_experiment_variants (
				id, experiment_id, prompt_version_id, label, weight, created_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`, uuid.NewString(), experimentID, variant.PromptVersionID, strings.TrimSpace(variant.Label),
			variant.Weight, now,
		); err != nil {
			return Experiment{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return Experiment{}, err
	}

	return r.GetExperiment(ctx, teamID, experimentID)
}

func (r *Repository) ListExperimentVariants(
	ctx context.Context,
	teamID int64,
	experimentID string,
) ([]ExperimentVariant, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			v.id,
			v.experiment_id,
			v.prompt_version_id,
			v.label,
			v.weight,
			v.created_at
		FROM ai_experiment_variants v
		INNER JOIN ai_experiments e ON e.id = v.experiment_id
		WHERE e.team_id = ? AND v.experiment_id = ?
		ORDER BY v.created_at ASC
	`, teamID, experimentID)
	if err != nil {
		return nil, err
	}
	return mapExperimentVariantRows(rows), nil
}

func (r *Repository) ListExperimentRuns(
	ctx context.Context,
	teamID int64,
	experimentID string,
) ([]ExperimentRun, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			experiment_id,
			status,
			COALESCE(winner_variant_id, '') AS winner_variant_id,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_experiment_runs
		WHERE team_id = ? AND experiment_id = ?
		ORDER BY created_at DESC
	`, teamID, experimentID)
	if err != nil {
		return nil, err
	}
	return mapExperimentRunRows(rows), nil
}

func (r *Repository) GetExperimentRun(ctx context.Context, runID string) (ExperimentRun, error) {
	row, err := dbutil.QueryMap(r.querier, `
		SELECT
			id,
			team_id,
			experiment_id,
			status,
			COALESCE(winner_variant_id, '') AS winner_variant_id,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_experiment_runs
		WHERE id = ?
		LIMIT 1
	`, runID)
	if err != nil {
		return ExperimentRun{}, err
	}
	if len(row) == 0 {
		return ExperimentRun{}, sql.ErrNoRows
	}
	return experimentRunFromRow(row), nil
}

func (r *Repository) QueueExperimentRun(
	ctx context.Context,
	teamID int64,
	experimentID string,
) (ExperimentRun, error) {
	if _, err := r.GetExperiment(ctx, teamID, experimentID); err != nil {
		return ExperimentRun{}, err
	}

	id := uuid.NewString()
	now := time.Now().UTC()
	_, err := r.querier.ExecContext(ctx, `
		INSERT INTO ai_experiment_runs (
			id, team_id, experiment_id, status, winner_variant_id, summary_json,
			started_at, finished_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, teamID, experimentID, "queued", nil, "{}", nil, nil, now)
	if err != nil {
		return ExperimentRun{}, err
	}
	return r.GetExperimentRun(ctx, id)
}

func (r *Repository) ListQueuedExperimentRuns(ctx context.Context, limit int) ([]ExperimentRun, error) {
	rows, err := dbutil.QueryMaps(r.querier, `
		SELECT
			id,
			team_id,
			experiment_id,
			status,
			COALESCE(winner_variant_id, '') AS winner_variant_id,
			COALESCE(summary_json, '{}') AS summary_json,
			COALESCE(started_at, NULL) AS started_at,
			COALESCE(finished_at, NULL) AS finished_at,
			created_at
		FROM ai_experiment_runs
		WHERE status = 'queued'
		ORDER BY created_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	return mapExperimentRunRows(rows), nil
}

func (r *Repository) MarkExperimentRunRunning(ctx context.Context, runID string) (bool, error) {
	res, err := r.querier.ExecContext(ctx, `
		UPDATE ai_experiment_runs
		SET status = 'running', started_at = ?
		WHERE id = ? AND status = 'queued'
	`, time.Now().UTC(), runID)
	if err != nil {
		return false, err
	}
	return dbutil.RowsAffected(res) > 0, nil
}

func (r *Repository) CompleteExperimentRun(
	ctx context.Context,
	runID string,
	winnerVariantID string,
	summary map[string]any,
) error {
	_, err := r.querier.ExecContext(ctx, `
		UPDATE ai_experiment_runs
		SET status = 'completed',
			winner_variant_id = ?,
			summary_json = ?,
			finished_at = ?
		WHERE id = ?
	`, nullableString(winnerVariantID), dbutil.JSONString(summary), time.Now().UTC(), runID)
	return err
}

func (r *Repository) FailExperimentRun(ctx context.Context, runID string, message string) error {
	_, err := r.querier.ExecContext(ctx, `
		UPDATE ai_experiment_runs
		SET status = 'failed',
			summary_json = ?,
			finished_at = ?
		WHERE id = ?
	`, dbutil.JSONString(map[string]any{"error": message}), time.Now().UTC(), runID)
	return err
}

func (r *Repository) datasetItemCount(ctx context.Context, datasetID string) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM ai_dataset_items
		WHERE dataset_id = ?
	`, datasetID).Scan(&count)
	return count, err
}

func mapPromptRows(rows []map[string]any) []Prompt {
	out := make([]Prompt, 0, len(rows))
	for _, row := range rows {
		out = append(out, promptFromRow(row))
	}
	return out
}

func promptFromRow(row map[string]any) Prompt {
	return Prompt{
		ID:              asString(row["id"]),
		TeamID:          asInt64(row["team_id"]),
		Name:            asString(row["name"]),
		Slug:            asString(row["slug"]),
		Description:     asString(row["description"]),
		ModelProvider:   asString(row["model_provider"]),
		ModelName:       asString(row["model_name"]),
		SystemPrompt:    asString(row["system_prompt"]),
		UserTemplate:    asString(row["user_template"]),
		Tags:            parseStringSlice(row["tags_json"]),
		LatestVersion:   int(asInt64(row["latest_version"])),
		ActiveVersionID: asString(row["active_version_id"]),
		UpdatedAt:       asString(row["updated_at"]),
		CreatedAt:       asString(row["created_at"]),
	}
}

func mapPromptVersionRows(rows []map[string]any) []PromptVersion {
	out := make([]PromptVersion, 0, len(rows))
	for _, row := range rows {
		out = append(out, promptVersionFromRow(row))
	}
	return out
}

func promptVersionFromRow(row map[string]any) PromptVersion {
	return PromptVersion{
		ID:            asString(row["id"]),
		PromptID:      asString(row["prompt_id"]),
		VersionNumber: int(asInt64(row["version_number"])),
		Changelog:     asString(row["changelog"]),
		SystemPrompt:  asString(row["system_prompt"]),
		UserTemplate:  asString(row["user_template"]),
		Variables:     parseStringSlice(row["variables_json"]),
		IsActive:      asBool(row["is_active"]),
		CreatedAt:     asString(row["created_at"]),
	}
}

func mapDatasetRows(rows []map[string]any) []Dataset {
	out := make([]Dataset, 0, len(rows))
	for _, row := range rows {
		out = append(out, datasetFromRow(row))
	}
	return out
}

func datasetFromRow(row map[string]any) Dataset {
	return Dataset{
		ID:          asString(row["id"]),
		TeamID:      asInt64(row["team_id"]),
		Name:        asString(row["name"]),
		Description: asString(row["description"]),
		Tags:        parseStringSlice(row["tags_json"]),
		ItemCount:   int(asInt64(row["item_count"])),
		UpdatedAt:   asString(row["updated_at"]),
		CreatedAt:   asString(row["created_at"]),
	}
}

func mapDatasetItemRows(rows []map[string]any) []DatasetItem {
	out := make([]DatasetItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, datasetItemFromRow(row))
	}
	return out
}

func datasetItemFromRow(row map[string]any) DatasetItem {
	return DatasetItem{
		ID:             asString(row["id"]),
		DatasetID:      asString(row["dataset_id"]),
		Input:          parseMap(row["input_json"]),
		ExpectedOutput: asString(row["expected_output"]),
		Metadata:       parseMap(row["metadata_json"]),
		CreatedAt:      asString(row["created_at"]),
	}
}

func mapFeedbackRows(rows []map[string]any) []Feedback {
	out := make([]Feedback, 0, len(rows))
	for _, row := range rows {
		out = append(out, feedbackFromRow(row))
	}
	return out
}

func feedbackFromRow(row map[string]any) Feedback {
	return Feedback{
		ID:         asString(row["id"]),
		TeamID:     asInt64(row["team_id"]),
		TargetType: asString(row["target_type"]),
		TargetID:   asString(row["target_id"]),
		RunSpanID:  asString(row["run_span_id"]),
		TraceID:    asString(row["trace_id"]),
		Score:      int(asInt64(row["score"])),
		Label:      asString(row["label"]),
		Comment:    asString(row["comment"]),
		CreatedBy:  asString(row["created_by"]),
		CreatedAt:  asString(row["created_at"]),
	}
}

func mapEvalRows(rows []map[string]any) []EvalSuite {
	out := make([]EvalSuite, 0, len(rows))
	for _, row := range rows {
		out = append(out, evalFromRow(row))
	}
	return out
}

func evalFromRow(row map[string]any) EvalSuite {
	return EvalSuite{
		ID:          asString(row["id"]),
		TeamID:      asInt64(row["team_id"]),
		Name:        asString(row["name"]),
		Description: asString(row["description"]),
		PromptID:    asString(row["prompt_id"]),
		DatasetID:   asString(row["dataset_id"]),
		JudgeModel:  asString(row["judge_model"]),
		Status:      asString(row["status"]),
		UpdatedAt:   asString(row["updated_at"]),
		CreatedAt:   asString(row["created_at"]),
	}
}

func mapEvalRunRows(rows []map[string]any) []EvalRun {
	out := make([]EvalRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, evalRunFromRow(row))
	}
	return out
}

func evalRunFromRow(row map[string]any) EvalRun {
	return EvalRun{
		ID:              asString(row["id"]),
		TeamID:          asInt64(row["team_id"]),
		EvalID:          asString(row["eval_id"]),
		PromptVersionID: asString(row["prompt_version_id"]),
		DatasetID:       asString(row["dataset_id"]),
		Status:          asString(row["status"]),
		AverageScore:    asFloat64(row["average_score"]),
		PassRate:        asFloat64(row["pass_rate"]),
		TotalCases:      int(asInt64(row["total_cases"])),
		CompletedCases:  int(asInt64(row["completed_cases"])),
		Summary:         parseMap(row["summary_json"]),
		StartedAt:       asString(row["started_at"]),
		FinishedAt:      asString(row["finished_at"]),
		CreatedAt:       asString(row["created_at"]),
	}
}

func mapEvalScoreRows(rows []map[string]any) []EvalScore {
	out := make([]EvalScore, 0, len(rows))
	for _, row := range rows {
		out = append(out, evalScoreFromRow(row))
	}
	return out
}

func evalScoreFromRow(row map[string]any) EvalScore {
	return EvalScore{
		ID:            asString(row["id"]),
		EvalRunID:     asString(row["eval_run_id"]),
		DatasetItemID: asString(row["dataset_item_id"]),
		Score:         asFloat64(row["score"]),
		ResultLabel:   asString(row["result_label"]),
		Reason:        asString(row["reason"]),
		OutputText:    asString(row["output_text"]),
		CreatedAt:     asString(row["created_at"]),
	}
}

func mapExperimentRows(rows []map[string]any) []Experiment {
	out := make([]Experiment, 0, len(rows))
	for _, row := range rows {
		out = append(out, experimentFromRow(row))
	}
	return out
}

func experimentFromRow(row map[string]any) Experiment {
	return Experiment{
		ID:          asString(row["id"]),
		TeamID:      asInt64(row["team_id"]),
		Name:        asString(row["name"]),
		Description: asString(row["description"]),
		DatasetID:   asString(row["dataset_id"]),
		Status:      asString(row["status"]),
		UpdatedAt:   asString(row["updated_at"]),
		CreatedAt:   asString(row["created_at"]),
	}
}

func mapExperimentVariantRows(rows []map[string]any) []ExperimentVariant {
	out := make([]ExperimentVariant, 0, len(rows))
	for _, row := range rows {
		out = append(out, experimentVariantFromRow(row))
	}
	return out
}

func experimentVariantFromRow(row map[string]any) ExperimentVariant {
	return ExperimentVariant{
		ID:              asString(row["id"]),
		ExperimentID:    asString(row["experiment_id"]),
		PromptVersionID: asString(row["prompt_version_id"]),
		Label:           asString(row["label"]),
		Weight:          asFloat64(row["weight"]),
		CreatedAt:       asString(row["created_at"]),
	}
}

func mapExperimentRunRows(rows []map[string]any) []ExperimentRun {
	out := make([]ExperimentRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, experimentRunFromRow(row))
	}
	return out
}

func experimentRunFromRow(row map[string]any) ExperimentRun {
	return ExperimentRun{
		ID:              asString(row["id"]),
		TeamID:          asInt64(row["team_id"]),
		ExperimentID:    asString(row["experiment_id"]),
		Status:          asString(row["status"]),
		WinnerVariantID: asString(row["winner_variant_id"]),
		Summary:         parseMap(row["summary_json"]),
		StartedAt:       asString(row["started_at"]),
		FinishedAt:      asString(row["finished_at"]),
		CreatedAt:       asString(row["created_at"]),
	}
}

func asString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func asInt64(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		i, _ := typed.Int64()
		return i
	default:
		var i int64
		_, _ = fmt.Sscan(asString(value), &i)
		return i
	}
}

func asFloat64(value any) float64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int64:
		return float64(typed)
	case int:
		return float64(typed)
	case json.Number:
		f, _ := typed.Float64()
		return f
	default:
		var f float64
		_, _ = fmt.Sscan(asString(value), &f)
		return f
	}
}

func asBool(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case int64:
		return typed != 0
	case int:
		return typed != 0
	case float64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func parseStringSlice(value any) []string {
	raw := strings.TrimSpace(asString(value))
	if raw == "" {
		return []string{}
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return []string{}
	}
	return out
}

func parseMap(value any) map[string]any {
	raw := strings.TrimSpace(asString(value))
	if raw == "" {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return map[string]any{}
	}
	return out
}

func extractVariables(template string) []string {
	matches := variablePattern.FindAllStringSubmatch(template, -1)
	if len(matches) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(matches))
	var out []string
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := strings.TrimSpace(match[1])
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func boolToTinyInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func requireNonEmpty(fields map[string]string) error {
	for name, value := range fields {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

func allowedFeedbackTarget(targetType string) bool {
	switch targetType {
	case "run", "prompt", "dataset-item", "eval-run", "experiment-run":
		return true
	default:
		return false
	}
}

func allowedEvalStatus(status string) bool {
	switch status {
	case "draft", "active", "paused":
		return true
	default:
		return false
	}
}

func ensureValidFeedbackTarget(targetType string) error {
	if !allowedFeedbackTarget(targetType) {
		return errors.New("targetType is invalid")
	}
	return nil
}
