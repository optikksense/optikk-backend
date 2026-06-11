package token

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/golang-jwt/jwt/v5"
)

const (
	typAccess  = "access"
	typRefresh = "refresh"
)

// AuthState carries the authenticated user identity embedded in tokens.
type AuthState struct {
	UserID        int64
	Email         string
	Role          string
	DefaultTeamID int64
	TeamIDs       []int64
}

type accessClaims struct {
	Typ           string  `json:"typ"`
	Email         string  `json:"email"`
	Role          string  `json:"role"`
	DefaultTeamID int64   `json:"dtid"`
	TeamIDs       []int64 `json:"tids"`
	jwt.RegisteredClaims
}

type refreshClaims struct {
	Typ string `json:"typ"`
	jwt.RegisteredClaims
}

// Service signs and verifies JWT access and refresh tokens.
type Service struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
	cookie     cookieOpts
}

// NewService creates a token service from the auth configuration.
func NewService(cfg config.Config) *Service {
	return &Service{
		secret:     []byte(cfg.Auth.JWTSecret),
		accessTTL:  cfg.AccessTokenTTL(),
		refreshTTL: cfg.RefreshTokenTTL(),
		cookie: cookieOpts{
			name:     cfg.Auth.RefreshCookieName,
			domain:   cfg.Auth.CookieDomain,
			secure:   cfg.Auth.CookieSecure,
			sameSite: parseSameSite(cfg.Auth.CookieSameSite),
		},
	}
}

// SignAccess issues a short-lived access token for the given auth state.
func (s *Service) SignAccess(state AuthState) (string, error) {
	now := time.Now()
	claims := accessClaims{
		Typ:           typAccess,
		Email:         state.Email,
		Role:          state.Role,
		DefaultTeamID: state.DefaultTeamID,
		TeamIDs:       state.TeamIDs,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strconv.FormatInt(state.UserID, 10),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

// ParseAccess verifies an access token and returns its auth state.
func (s *Service) ParseAccess(raw string) (AuthState, error) {
	var claims accessClaims
	if err := s.parse(raw, &claims); err != nil {
		return AuthState{}, err
	}
	if claims.Typ != typAccess {
		return AuthState{}, fmt.Errorf("token is not an access token")
	}
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil || userID == 0 {
		return AuthState{}, fmt.Errorf("invalid token subject")
	}
	return AuthState{
		UserID:        userID,
		Email:         claims.Email,
		Role:          claims.Role,
		DefaultTeamID: claims.DefaultTeamID,
		TeamIDs:       claims.TeamIDs,
	}, nil
}

// SignRefresh issues a long-lived refresh token carrying only the user ID.
func (s *Service) SignRefresh(userID int64) (string, error) {
	now := time.Now()
	claims := refreshClaims{
		Typ: typRefresh,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strconv.FormatInt(userID, 10),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.refreshTTL)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
}

// ParseRefresh verifies a refresh token and returns the user ID.
func (s *Service) ParseRefresh(raw string) (int64, error) {
	var claims refreshClaims
	if err := s.parse(raw, &claims); err != nil {
		return 0, err
	}
	if claims.Typ != typRefresh {
		return 0, fmt.Errorf("token is not a refresh token")
	}
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil || userID == 0 {
		return 0, fmt.Errorf("invalid token subject")
	}
	return userID, nil
}

func (s *Service) parse(raw string, claims jwt.Claims) error {
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithLeeway(30*time.Second),
		jwt.WithExpirationRequired(),
	)
	_, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		return s.secret, nil
	})
	return err
}
