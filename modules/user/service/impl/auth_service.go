package impl

import (
	"strings"
	"time"

	serviceinterfaces "github.com/observability/observability-backend-go/modules/user/service/interfaces"
	"github.com/observability/observability-backend-go/modules/user/store"
	"golang.org/x/crypto/bcrypt"
)

// JWTGenerator is the subset of JWT manager required by AuthService.
type JWTGenerator interface {
	Generate(userID int64, email, name, role string, teamID int64, teams ...int64) (string, error)
}

type authService struct {
	tables       store.TableProvider
	jwtGenerator JWTGenerator
	jwtExpiresMs int64
}

func NewAuthService(tables store.TableProvider, jwtGenerator JWTGenerator, jwtExpiresMs int64) serviceinterfaces.AuthService {
	return &authService{
		tables:       tables,
		jwtGenerator: jwtGenerator,
		jwtExpiresMs: jwtExpiresMs,
	}
}

func (s *authService) Login(email, password string) (map[string]any, error) {
	if strings.TrimSpace(email) == "" || strings.TrimSpace(password) == "" {
		return nil, newValidationError("Email and password are required", nil)
	}

	user, err := s.tables.Users().FindActiveByEmail(strings.TrimSpace(email))
	if err != nil {
		return nil, newValidationError("Invalid email or password", err)
	}

	if user.PasswordHash != "" {
		if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
			return nil, newValidationError("Invalid email or password", nil)
		}
	}

	_ = s.tables.Users().UpdateLastLogin(user.ID, time.Now().UTC())

	teams, err := listActiveTeamsForUser(s.tables, user.ID)
	if err != nil {
		teams = []map[string]any{}
	}

	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}

	teamID := int64(1)
	if currentTeam != nil {
		teamID = mapInt64(currentTeam, "id")
	}

	var teamIDs []int64
	for _, t := range teams {
		teamIDs = append(teamIDs, mapInt64(t, "id"))
	}

	token, err := s.jwtGenerator.Generate(user.ID, user.Email, user.Name, user.Role, teamID, teamIDs...)
	if err != nil {
		return nil, newInternalError("Failed to generate token", err)
	}

	return map[string]any{
		"token":       token,
		"tokenType":   "Bearer",
		"expiresIn":   s.jwtExpiresMs,
		"user":        map[string]any{"id": user.ID, "email": user.Email, "name": user.Name, "avatarUrl": user.AvatarURL, "role": user.Role},
		"teams":       teams,
		"currentTeam": currentTeam,
	}, nil
}

func (s *authService) BuildAuthContext(userID int64) (map[string]any, error) {
	if userID == 0 {
		return nil, newUnauthorizedError("Not authenticated", nil)
	}

	ctx, err := buildAuthContextResponse(s.tables, userID)
	if err != nil {
		return nil, newUnauthorizedError("Not authenticated", err)
	}

	return ctx, nil
}
