package user

import (
	sessionauth "github.com/Optikk-Org/optikk-backend/internal/modules/session"
)

// Service manages user, team, and auth-related operations.
type Service struct {
	repo     *Repository
	sessions sessionauth.Manager
}

// NewService creates a new Service instance.
func NewService(repo *Repository, sessions sessionauth.Manager) *Service {
	return &Service{
		repo:     repo,
		sessions: sessions,
	}
}
