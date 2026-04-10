package session

import (
	"context"
	"net/http"
)

type AuthState struct {
	UserID        int64
	Email         string
	Role          string
	DefaultTeamID int64
	TeamIDs       []int64
}

type Manager interface {
	Wrap(next http.Handler) http.Handler
	CreateAuthSession(ctx context.Context, state AuthState) error
	DestroySession(ctx context.Context) error
	GetAuthState(ctx context.Context) (AuthState, bool)
}
