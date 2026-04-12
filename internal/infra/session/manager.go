package session

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/alexedwards/scs/redisstore"
	"github.com/alexedwards/scs/v2"
	redigoredis "github.com/gomodule/redigo/redis"
)

const (
	authUserIDKey        = "auth_user_id"
	authEmailKey         = "auth_email"
	authRoleKey          = "auth_role"
	authDefaultTeamIDKey = "auth_default_team_id"
	authTeamIDsKey       = "auth_team_ids"
)

type SessionManager struct {
	*scs.SessionManager
}

func NewManager(cfg config.Config, pool *redigoredis.Pool) (*SessionManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("redis pool is required for sessions")
	}
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
	sessionManager.Store = redisstore.New(pool)

	return &SessionManager{
		SessionManager: sessionManager,
	}, nil
}

func (m *SessionManager) Wrap(next http.Handler) http.Handler {
	return m.LoadAndSave(next)
}

func (m *SessionManager) CreateAuthSession(ctx context.Context, state AuthState) error {
	if err := m.RenewToken(ctx); err != nil {
		return err
	}
	m.clearAuthState(ctx)

	m.Put(ctx, authUserIDKey, state.UserID)
	m.Put(ctx, authEmailKey, state.Email)
	m.Put(ctx, authRoleKey, state.Role)
	m.Put(ctx, authDefaultTeamIDKey, state.DefaultTeamID)
	m.Put(ctx, authTeamIDsKey, encodeInt64List(state.TeamIDs))
	return nil
}

func (m *SessionManager) DestroySession(ctx context.Context) error {
	return m.Destroy(ctx)
}

func (m *SessionManager) GetAuthState(ctx context.Context) (AuthState, bool) {
	userID := m.GetInt64(ctx, authUserIDKey)
	if userID == 0 {
		return AuthState{}, false
	}

	state := AuthState{
		UserID:        userID,
		Email:         m.GetString(ctx, authEmailKey),
		Role:          m.GetString(ctx, authRoleKey),
		DefaultTeamID: m.GetInt64(ctx, authDefaultTeamIDKey),
		TeamIDs:       decodeInt64List(m.GetString(ctx, authTeamIDsKey)),
	}

	if len(state.TeamIDs) == 0 && state.DefaultTeamID > 0 {
		state.TeamIDs = []int64{state.DefaultTeamID}
	}

	return state, true
}

func (m *SessionManager) clearAuthState(ctx context.Context) {
	m.Remove(ctx, authUserIDKey)
	m.Remove(ctx, authEmailKey)
	m.Remove(ctx, authRoleKey)
	m.Remove(ctx, authDefaultTeamIDKey)
	m.Remove(ctx, authTeamIDsKey)
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
