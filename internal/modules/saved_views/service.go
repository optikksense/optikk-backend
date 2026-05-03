package savedviews

import (
	"context"
	"errors"
	"strings"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) List(ctx context.Context, teamID int64, scope string) ([]SavedView, error) {
	return s.repo.List(ctx, teamID, scope)
}

func (s *Service) Create(ctx context.Context, teamID, userID int64, req CreateRequest) (SavedView, error) {
	visibility := strings.ToLower(strings.TrimSpace(req.Visibility))
	if visibility != "team" {
		visibility = "private"
	}
	if strings.TrimSpace(req.Scope) == "" {
		return SavedView{}, errors.New("scope is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		return SavedView{}, errors.New("name is required")
	}
	if strings.TrimSpace(req.URL) == "" {
		return SavedView{}, errors.New("url is required")
	}
	return s.repo.Create(ctx, SavedView{
		TeamID:     teamID,
		UserID:     userID,
		Scope:      req.Scope,
		Name:       req.Name,
		URL:        req.URL,
		Visibility: visibility,
	})
}

func (s *Service) Delete(ctx context.Context, teamID, userID, id int64) error {
	return s.repo.Delete(ctx, teamID, userID, id)
}
