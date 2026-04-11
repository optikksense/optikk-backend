package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Service implements LLM hub use cases.
type Service struct {
	repo Repository
}

// NewService constructs the hub service.
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateScore(ctx context.Context, teamID int64, req CreateScoreRequest) (int64, error) {
	if err := validateScore(req); err != nil {
		return 0, err
	}
	return s.repo.InsertScore(ctx, teamID, req)
}

func validateScore(req CreateScoreRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(req.TraceID) == "" {
		return errors.New("trace_id is required")
	}
	return nil
}

func (s *Service) BatchCreateScores(ctx context.Context, teamID int64, scores []CreateScoreRequest) ([]int64, error) {
	if len(scores) == 0 {
		return nil, errors.New("scores array is empty")
	}
	if len(scores) > 200 {
		return nil, errors.New("max 200 scores per batch")
	}
	ids := make([]int64, 0, len(scores))
	for _, sc := range scores {
		id, err := s.CreateScore(ctx, teamID, sc)
		if err != nil {
			return ids, fmt.Errorf("score %d: %w", len(ids), err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Service) ListScores(ctx context.Context, teamID, startMs, endMs int64, name, traceID string, limit, offset int) (ListScoresResponse, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, total, err := s.repo.ListScores(ctx, teamID, startMs, endMs, name, traceID, limit, offset)
	if err != nil {
		return ListScoresResponse{}, err
	}
	out := make([]ScoreDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, scoreRowToDTO(r))
	}
	return ListScoresResponse{
		Results:  out,
		PageInfo: PageInfo{Total: total, Offset: offset, Limit: limit},
	}, nil
}

func scoreRowToDTO(r scoreRow) ScoreDTO {
	d := ScoreDTO{
		ID:        r.ID,
		Name:      r.Name,
		Value:     r.Value,
		TraceID:   r.TraceID,
		SpanID:    r.SpanID,
		Source:    r.Source,
		CreatedAt: r.CreatedAt,
	}
	if r.SessionID.Valid {
		d.SessionID = r.SessionID.String
	}
	if r.PromptTemplate.Valid {
		d.PromptTemplate = r.PromptTemplate.String
	}
	if r.Model.Valid {
		d.Model = r.Model.String
	}
	if r.Rationale.Valid {
		d.Rationale = r.Rationale.String
	}
	return d
}

func promptRowToDTO(r promptRow) PromptDTO {
	d := PromptDTO{
		ID:          r.ID,
		Slug:        r.Slug,
		DisplayName: r.DisplayName,
		Body:        r.Body,
		Version:     r.Version,
		CreatedAt:   r.CreatedAt,
	}
	if r.UpdatedAt.Valid {
		t := r.UpdatedAt.Time
		d.UpdatedAt = &t
	}
	return d
}

func datasetRowToDTO(r datasetRow) DatasetDTO {
	d := DatasetDTO{
		ID:          r.ID,
		Name:        r.Name,
		StartTimeMs: r.StartTimeMs,
		EndTimeMs:   r.EndTimeMs,
		RowCount:    r.RowCount,
		CreatedAt:   r.CreatedAt,
	}
	if r.QuerySnapshot.Valid {
		d.QuerySnapshot = r.QuerySnapshot.String
	}
	return d
}

// --- Prompts ---

func (s *Service) ListPrompts(ctx context.Context, teamID int64) (ListPromptsResponse, error) {
	rows, err := s.repo.ListPrompts(ctx, teamID)
	if err != nil {
		return ListPromptsResponse{}, err
	}
	out := make([]PromptDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, promptRowToDTO(r))
	}
	return ListPromptsResponse{Results: out}, nil
}

func (s *Service) CreatePrompt(ctx context.Context, teamID int64, req CreatePromptRequest) (PromptDTO, error) {
	if strings.TrimSpace(req.Slug) == "" || strings.TrimSpace(req.DisplayName) == "" || strings.TrimSpace(req.Body) == "" {
		return PromptDTO{}, errors.New("slug, display_name, and body are required")
	}
	id, err := s.repo.InsertPrompt(ctx, teamID, req)
	if err != nil {
		return PromptDTO{}, err
	}
	rows, err := s.repo.ListPrompts(ctx, teamID)
	if err != nil {
		return PromptDTO{}, err
	}
	for _, r := range rows {
		if r.ID == id {
			return promptRowToDTO(r), nil
		}
	}
	return PromptDTO{}, errors.New("prompt not found after insert")
}

func (s *Service) UpdatePrompt(ctx context.Context, teamID, id int64, req UpdatePromptRequest) error {
	if req.DisplayName == nil && req.Body == nil {
		return errors.New("nothing to update")
	}
	return s.repo.UpdatePrompt(ctx, teamID, id, req)
}

func (s *Service) DeletePrompt(ctx context.Context, teamID, id int64) error {
	return s.repo.DeletePrompt(ctx, teamID, id)
}

// --- Datasets ---

func (s *Service) ListDatasets(ctx context.Context, teamID int64, limit int) (ListDatasetsResponse, error) {
	rows, err := s.repo.ListDatasets(ctx, teamID, limit)
	if err != nil {
		return ListDatasetsResponse{}, err
	}
	out := make([]DatasetDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, datasetRowToDTO(r))
	}
	return ListDatasetsResponse{Results: out}, nil
}

func (s *Service) CreateDataset(ctx context.Context, teamID int64, req CreateDatasetRequest) (DatasetDTO, error) {
	if strings.TrimSpace(req.Name) == "" {
		return DatasetDTO{}, errors.New("name is required")
	}
	if len(req.Generations) == 0 {
		return DatasetDTO{}, errors.New("generations is empty")
	}
	if len(req.Generations) > 5000 {
		return DatasetDTO{}, errors.New("max 5000 rows per dataset")
	}
	raw, err := json.Marshal(req.Generations)
	if err != nil {
		return DatasetDTO{}, err
	}
	id, err := s.repo.InsertDataset(ctx, teamID, req.Name, req.QuerySnapshot, req.StartTimeMs, req.EndTimeMs, string(raw), len(req.Generations))
	if err != nil {
		return DatasetDTO{}, err
	}
	row, err := s.repo.GetDataset(ctx, teamID, id)
	if err != nil {
		return DatasetDTO{}, err
	}
	return datasetRowToDTO(*row), nil
}

func (s *Service) GetDataset(ctx context.Context, teamID, id int64) (DatasetDetailDTO, error) {
	row, err := s.repo.GetDataset(ctx, teamID, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DatasetDetailDTO{}, err
		}
		return DatasetDetailDTO{}, err
	}
	return DatasetDetailDTO{
		DatasetDTO:  datasetRowToDTO(*row),
		PayloadJSON: row.PayloadJSON,
	}, nil
}

// --- Settings ---

func (s *Service) GetTeamSettings(ctx context.Context, teamID int64) (TeamSettingsDTO, error) {
	raw, updated, err := s.repo.GetTeamSettings(ctx, teamID)
	if err != nil {
		return TeamSettingsDTO{}, err
	}
	out := TeamSettingsDTO{
		PricingOverrides: map[string]PricingOverride{},
		UpdatedAt:        updated,
	}
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out.PricingOverrides); err != nil {
		return TeamSettingsDTO{}, err
	}
	return out, nil
}

func (s *Service) PatchTeamSettings(ctx context.Context, teamID int64, req PatchTeamSettingsRequest) error {
	if req.PricingOverrides == nil {
		return errors.New("pricing_overrides is required")
	}
	raw, err := json.Marshal(req.PricingOverrides)
	if err != nil {
		return err
	}
	return s.repo.UpsertTeamSettings(ctx, teamID, raw)
}
