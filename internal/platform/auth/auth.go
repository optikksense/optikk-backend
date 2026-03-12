package auth

import (
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/observability/observability-backend-go/internal/platform/utils"
)

type JWTManager struct {
	Secret     []byte
	Expiration time.Duration
}

type cachedClaims struct {
	claims    *TokenClaims
	expiresAt time.Time
}

const claimsCacheTTL = 10 * time.Second

// Package-level claims cache — safe to use from value-receiver methods
// since sync.Map must not be copied.
var jwtClaimsCache sync.Map

type TokenClaims struct {
	Email  string `json:"email"`
	Name   string `json:"name"`
	Role   string `json:"role"`
	TeamID int64  `json:"teamId"`
	// Teams lists all team IDs this user is a member of. Used by TenantMiddleware
	// to validate X-Team-Id header overrides. Empty slice = legacy token (allow override).
	Teams []int64 `json:"teams,omitempty"`
	jwt.RegisteredClaims
}

func (m JWTManager) Generate(userID int64, email, name, role string, teamID int64, teams ...int64) (string, error) {
	now := time.Now().UTC()
	// Ensure the primary teamID is always included in the Teams list.
	allTeams := teams
	if len(allTeams) == 0 && teamID > 0 {
		allTeams = []int64{teamID}
	}
	claims := TokenClaims{
		Email:  email,
		Name:   name,
		Role:   role,
		TeamID: teamID,
		Teams:  allTeams,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   utils.ToString(userID),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.Expiration)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.Secret)
}

// ClaimsAuthorizedForTeam returns true if the claims permit access to the
// requested team. Legacy tokens with an empty Teams list are allowed through
// for backward compatibility (they only contain the single primary team).
func ClaimsAuthorizedForTeam(claims *TokenClaims, requestedTeamID int64) bool {
	if len(claims.Teams) == 0 {
		// Legacy token: only allow the primary team.
		return claims.TeamID == requestedTeamID
	}
	for _, tid := range claims.Teams {
		if tid == requestedTeamID {
			return true
		}
	}
	return false
}

type PendingOAuthClaims struct {
	PendingOAuth bool   `json:"pending_oauth"`
	Provider     string `json:"oauth_provider"`
	OAuthID      string `json:"oauth_id"`
	Email        string `json:"email"`
	Name         string `json:"name"`
	AvatarURL    string `json:"avatar_url"`
	jwt.RegisteredClaims
}

// GeneratePendingOAuthToken issues a short-lived (10 min) token for an OAuth user
// who doesn't have an account yet and needs to complete signup with team/org info.
func (m JWTManager) GeneratePendingOAuthToken(provider, oauthID, email, name, avatarURL string) (string, error) {
	now := time.Now().UTC()
	claims := PendingOAuthClaims{
		PendingOAuth: true,
		Provider:     provider,
		OAuthID:      oauthID,
		Email:        email,
		Name:         name,
		AvatarURL:    avatarURL,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(10 * time.Minute)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.Secret)
}

// ParsePendingOAuthToken validates and extracts claims from a pending OAuth token.
func (m JWTManager) ParsePendingOAuthToken(token string) (*PendingOAuthClaims, error) {
	parsed, err := jwt.ParseWithClaims(token, &PendingOAuthClaims{}, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return m.Secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*PendingOAuthClaims)
	if !ok || !parsed.Valid || !claims.PendingOAuth {
		return nil, errors.New("invalid pending OAuth token")
	}
	return claims, nil
}

func (m JWTManager) Parse(token string) (*TokenClaims, error) {
	// Fast path: check claims cache (lock-free read).
	cacheKey := token
	if len(cacheKey) > 32 {
		cacheKey = cacheKey[len(cacheKey)-32:]
	}
	if val, ok := jwtClaimsCache.Load(cacheKey); ok {
		cc := val.(*cachedClaims)
		if time.Now().Before(cc.expiresAt) {
			return cc.claims, nil
		}
	}

	parsed, err := jwt.ParseWithClaims(token, &TokenClaims{}, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return m.Secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*TokenClaims)
	if !ok || !parsed.Valid {
		return nil, errors.New("invalid token")
	}

	jwtClaimsCache.Store(cacheKey, &cachedClaims{
		claims:    claims,
		expiresAt: time.Now().Add(claimsCacheTTL),
	})
	return claims, nil
}
