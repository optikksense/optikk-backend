package trace_suggest //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

const (
	defaultLimit = 10
	maxLimit     = 50
)

// scalarFields is the canonical name set served by the scalar suggest path.
// Attribute keys (prefixed `@`) take a different path. Validation lives at
// the service boundary so repository.go stays as queries only.
var scalarFields = map[string]struct{}{
	"service":     {},
	"operation":   {},
	"http_method": {},
	"http_status": {},
	"status":      {},
	"environment": {},
}

// IsScalarField reports whether the field is a known scalar dimension.
// Exported for the handler's early-validation guard.
func IsScalarField(field string) bool {
	_, ok := scalarFields[field]
	return ok
}

type Service interface {
	Suggest(ctx context.Context, req SuggestRequest, teamID int64) (SuggestResponse, error)
}

type service struct {
	repo Repository
}

func NewService(repo Repository) Service { return &service{repo: repo} }

func (s *service) Suggest(ctx context.Context, req SuggestRequest, teamID int64) (SuggestResponse, error) {
	limit := pickLimit(req.Limit)
	rows, err := s.fetch(ctx, teamID, req, limit)
	if err != nil {
		slog.ErrorContext(ctx, "trace_suggest: Suggest failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("field", req.Field))
		return SuggestResponse{}, err
	}
	return SuggestResponse{Suggestions: toSuggestions(rows)}, nil
}

// fetch dispatches between the attribute path (prefix `@`) and the scalar
// path. Scalar fields are validated against the canonical set up-front so
// the repo can assume a known column name.
func (s *service) fetch(ctx context.Context, teamID int64, req SuggestRequest, limit int) ([]suggestionRow, error) {
	if strings.HasPrefix(req.Field, "@") {
		return s.repo.SuggestAttribute(ctx, teamID, req.StartTime, req.EndTime, req.Field, req.Prefix, limit)
	}
	if !IsScalarField(req.Field) {
		return nil, fmt.Errorf("trace_suggest: unknown scalar field %q", req.Field)
	}
	return s.repo.SuggestScalar(ctx, teamID, req.StartTime, req.EndTime, req.Field, req.Prefix, limit)
}

func toSuggestions(rows []suggestionRow) []Suggestion {
	out := make([]Suggestion, len(rows))
	for i, r := range rows {
		out[i] = Suggestion{Value: r.Value, Count: r.Count}
	}
	return out
}

func pickLimit(v int) int {
	if v <= 0 {
		return defaultLimit
	}
	if v > maxLimit {
		return maxLimit
	}
	return v
}
