package session

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/alexedwards/scs/v2"
)

const (
	authUserIDKey        = "auth_user_id"
	authEmailKey         = "auth_email"
	authRoleKey          = "auth_role"
	authDefaultTeamIDKey = "auth_default_team_id"
	authTeamIDsKey       = "auth_team_ids"
)

// Service manages the application HTTP sessions.
type Service struct {
	*scs.SessionManager
}

// NewService creates and configures a new session service.
func NewService(cfg config.Config, repo *Repository) *Service {
	sessionManager := scs.New()
	sessionManager.Lifetime = cfg.SessionLifetime()
	sessionManager.IdleTimeout = cfg.SessionIdleTimeout()
	sessionManager.HashTokenInStore = true
	sessionManager.Cookie.Name = cfg.Session.CookieName
	sessionManager.Cookie.Domain = cfg.Session.CookieDomain
	sessionManager.Cookie.Path = cfg.Session.CookiePath
	sessionManager.Cookie.HttpOnly = cfg.Session.CookieHTTPOnly
	sessionManager.Cookie.Secure = cfg.Session.CookieSecure
	sessionManager.Cookie.SameSite = parseSameSite(cfg.Session.CookieSameSite)
	sessionManager.Cookie.Persist = true
	sessionManager.Store = repo.Store

	return &Service{
		SessionManager: sessionManager,
	}
}

// Wrap wraps an http.Handler with session middleware.
func (s *Service) Wrap(next http.Handler) http.Handler {
	return s.LoadAndSave(next)
}

// CreateAuthSession creates a session for the authenticated user.
func (s *Service) CreateAuthSession(ctx context.Context, state AuthState) error {
	if err := s.RenewToken(ctx); err != nil {
		return err
	}
	s.clearAuthState(ctx)

	s.Put(ctx, authUserIDKey, state.UserID)
	s.Put(ctx, authEmailKey, state.Email)
	s.Put(ctx, authRoleKey, state.Role)
	s.Put(ctx, authDefaultTeamIDKey, state.DefaultTeamID)
	s.Put(ctx, authTeamIDsKey, encodeInt64List(state.TeamIDs))
	return nil
}

// DestroySession terminates the current session.
func (s *Service) DestroySession(ctx context.Context) error {
	return s.Destroy(ctx)
}

// GetAuthState retrieves the AuthState from the current session.
func (s *Service) GetAuthState(ctx context.Context) (AuthState, bool) {
	userID := s.GetInt64(ctx, authUserIDKey)
	if userID == 0 {
		return AuthState{}, false
	}

	state := AuthState{
		UserID:        userID,
		Email:         s.GetString(ctx, authEmailKey),
		Role:          s.GetString(ctx, authRoleKey),
		DefaultTeamID: s.GetInt64(ctx, authDefaultTeamIDKey),
		TeamIDs:       decodeInt64List(s.GetString(ctx, authTeamIDsKey)),
	}

	if len(state.TeamIDs) == 0 && state.DefaultTeamID > 0 {
		state.TeamIDs = []int64{state.DefaultTeamID}
	}

	return state, true
}

func (s *Service) clearAuthState(ctx context.Context) {
	s.Remove(ctx, authUserIDKey)
	s.Remove(ctx, authEmailKey)
	s.Remove(ctx, authRoleKey)
	s.Remove(ctx, authDefaultTeamIDKey)
	s.Remove(ctx, authTeamIDsKey)
}

func encodeInt64List(values []int64) string {
	if len(values) == 0 {
		return ""
	}

	parts := make([]string, 0, len(values))
	for _, value := range values {
		if value > 0 {
			parts = append(parts, strconv.FormatInt(value, 10))
		}
	}
	return strings.Join(parts, ",")
}

func decodeInt64List(raw string) []int64 {
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	values := make([]int64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil && value > 0 {
			values = append(values, value)
		}
	}
	return values
}

func parseSameSite(raw string) http.SameSite {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "strict":
		return http.SameSiteStrictMode
	case "none":
		return http.SameSiteNoneMode
	case "default":
		return http.SameSiteDefaultMode
	case "":
		return http.SameSiteLaxMode
	default:
		return http.SameSiteLaxMode
	}
}
