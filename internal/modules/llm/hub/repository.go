package hub

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Repository is MySQL persistence for the LLM hub (scores, prompts, datasets, settings).
type Repository interface {
	InsertScore(ctx context.Context, teamID int64, req CreateScoreRequest) (int64, error)
	ListScores(ctx context.Context, teamID, startMs, endMs int64, name, traceID string, limit, offset int) ([]scoreRow, int64, error)

	ListPrompts(ctx context.Context, teamID int64) ([]promptRow, error)
	InsertPrompt(ctx context.Context, teamID int64, req CreatePromptRequest) (int64, error)
	UpdatePrompt(ctx context.Context, teamID, id int64, req UpdatePromptRequest) error
	DeletePrompt(ctx context.Context, teamID, id int64) error

	ListDatasets(ctx context.Context, teamID int64, limit int) ([]datasetRow, error)
	InsertDataset(ctx context.Context, teamID int64, name, querySnapshot string, startMs, endMs int64, payloadJSON string, rowCount int) (int64, error)
	GetDataset(ctx context.Context, teamID, id int64) (*datasetRow, error)

	GetTeamSettings(ctx context.Context, teamID int64) ([]byte, *time.Time, error)
	UpsertTeamSettings(ctx context.Context, teamID int64, pricingJSON []byte) error
}

type mysqlRepository struct {
	db *sql.DB
}

// NewRepository builds a MySQL-backed hub repository.
func NewRepository(db *sql.DB) Repository {
	return &mysqlRepository{db: db}
}

func (r *mysqlRepository) InsertScore(ctx context.Context, teamID int64, req CreateScoreRequest) (int64, error) {
	src := req.Source
	if strings.TrimSpace(src) == "" {
		src = "api"
	}
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO llm_scores (team_id, name, value, trace_id, span_id, session_id, prompt_template, model, source, rationale, created_at)
		VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), UTC_TIMESTAMP())
	`, teamID, req.Name, req.Value, req.TraceID, req.SpanID, req.SessionID, req.PromptTemplate, req.Model, src, req.Rationale)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *mysqlRepository) ListScores(ctx context.Context, teamID, startMs, endMs int64, name, traceID string, limit, offset int) ([]scoreRow, int64, error) {
	start := time.UnixMilli(startMs).UTC()
	end := time.UnixMilli(endMs).UTC()

	where := `team_id = ? AND created_at >= ? AND created_at <= ?`
	args := []any{teamID, start, end}
	if strings.TrimSpace(name) != "" {
		where += ` AND name = ?`
		args = append(args, name)
	}
	if strings.TrimSpace(traceID) != "" {
		where += ` AND trace_id = ?`
		args = append(args, traceID)
	}

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM llm_scores WHERE %s`, where)
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listQuery := fmt.Sprintf(`
		SELECT id, team_id, name, value, trace_id, span_id, session_id, prompt_template, model, source, rationale, created_at
		FROM llm_scores WHERE %s ORDER BY created_at DESC LIMIT ? OFFSET ?`, where)
	args = append(args, limit, offset)

	rows, err := r.db.QueryContext(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var out []scoreRow
	for rows.Next() {
		var s scoreRow
		if err := rows.Scan(&s.ID, &s.TeamID, &s.Name, &s.Value, &s.TraceID, &s.SpanID, &s.SessionID, &s.PromptTemplate, &s.Model, &s.Source, &s.Rationale, &s.CreatedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, s)
	}
	return out, total, rows.Err()
}

func (r *mysqlRepository) ListPrompts(ctx context.Context, teamID int64) ([]promptRow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, team_id, slug, display_name, body, version, created_at, updated_at
		FROM llm_prompts WHERE team_id = ? ORDER BY display_name ASC`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []promptRow
	for rows.Next() {
		var p promptRow
		if err := rows.Scan(&p.ID, &p.TeamID, &p.Slug, &p.DisplayName, &p.Body, &p.Version, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *mysqlRepository) InsertPrompt(ctx context.Context, teamID int64, req CreatePromptRequest) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO llm_prompts (team_id, slug, display_name, body, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, 1, UTC_TIMESTAMP(), UTC_TIMESTAMP())`,
		teamID, strings.TrimSpace(req.Slug), req.DisplayName, req.Body)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *mysqlRepository) UpdatePrompt(ctx context.Context, teamID, id int64, req UpdatePromptRequest) error {
	var displayName, body string
	var version int
	err := r.db.QueryRowContext(ctx, `
		SELECT display_name, body, version FROM llm_prompts WHERE id = ? AND team_id = ? LIMIT 1`, id, teamID).Scan(&displayName, &body, &version)
	if err != nil {
		return err
	}
	if req.DisplayName != nil {
		displayName = *req.DisplayName
	}
	if req.Body != nil {
		body = *req.Body
		version++
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE llm_prompts SET display_name = ?, body = ?, version = ?, updated_at = UTC_TIMESTAMP()
		WHERE id = ? AND team_id = ?`, displayName, body, version, id, teamID)
	return err
}

func (r *mysqlRepository) DeletePrompt(ctx context.Context, teamID, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM llm_prompts WHERE id = ? AND team_id = ?`, id, teamID)
	return err
}

func (r *mysqlRepository) ListDatasets(ctx context.Context, teamID int64, limit int) ([]datasetRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, team_id, name, query_snapshot, start_time_ms, end_time_ms, row_count, created_at
		FROM llm_datasets WHERE team_id = ? ORDER BY created_at DESC LIMIT ?`, teamID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []datasetRow
	for rows.Next() {
		var d datasetRow
		if err := rows.Scan(&d.ID, &d.TeamID, &d.Name, &d.QuerySnapshot, &d.StartTimeMs, &d.EndTimeMs, &d.RowCount, &d.CreatedAt); err != nil {
			return nil, err
		}
		d.PayloadJSON = ""
		out = append(out, d)
	}
	return out, rows.Err()
}

func (r *mysqlRepository) InsertDataset(ctx context.Context, teamID int64, name, querySnapshot string, startMs, endMs int64, payloadJSON string, rowCount int) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO llm_datasets (team_id, name, query_snapshot, start_time_ms, end_time_ms, row_count, payload_json, created_at)
		VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?, UTC_TIMESTAMP())`,
		teamID, name, querySnapshot, startMs, endMs, rowCount, payloadJSON)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (r *mysqlRepository) GetDataset(ctx context.Context, teamID, id int64) (*datasetRow, error) {
	var d datasetRow
	err := r.db.QueryRowContext(ctx, `
		SELECT id, team_id, name, query_snapshot, start_time_ms, end_time_ms, row_count, payload_json, created_at
		FROM llm_datasets WHERE id = ? AND team_id = ? LIMIT 1`, id, teamID).Scan(
		&d.ID, &d.TeamID, &d.Name, &d.QuerySnapshot, &d.StartTimeMs, &d.EndTimeMs, &d.RowCount, &d.PayloadJSON, &d.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *mysqlRepository) GetTeamSettings(ctx context.Context, teamID int64) ([]byte, *time.Time, error) {
	var raw sql.NullString
	var updated sql.NullTime
	err := r.db.QueryRowContext(ctx, `
		SELECT pricing_overrides_json, updated_at FROM teams WHERE id = ? LIMIT 1`, teamID).Scan(&raw, &updated)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if !raw.Valid || raw.String == "" || raw.String == "null" {
		return nil, nullTimePtr(updated), nil
	}
	return []byte(raw.String), nullTimePtr(updated), nil
}

func nullTimePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time
	return &t
}

func (r *mysqlRepository) UpsertTeamSettings(ctx context.Context, teamID int64, pricingJSON []byte) error {
	s := string(pricingJSON)
	res, err := r.db.ExecContext(ctx, `
		UPDATE teams SET pricing_overrides_json = ?, updated_at = UTC_TIMESTAMP() WHERE id = ?`,
		s, teamID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
