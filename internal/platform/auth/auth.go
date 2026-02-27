package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/observability/observability-backend-go/internal/platform/utils"
)

type JWTManager struct {
	Secret     []byte
	Expiration time.Duration
}

type TokenClaims struct {
	Email  string `json:"email"`
	Name   string `json:"name"`
	Role   string `json:"role"`
	TeamID int64  `json:"teamId"`
	jwt.RegisteredClaims
}

func (m JWTManager) Generate(userID int64, email, name, role string, teamID int64) (string, error) {
	now := time.Now().UTC()
	claims := TokenClaims{
		Email:  email,
		Name:   name,
		Role:   role,
		TeamID: teamID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   utils.ToString(userID),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.Expiration)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.Secret)
}

func (m JWTManager) Parse(token string) (*TokenClaims, error) {
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
	return claims, nil
}
