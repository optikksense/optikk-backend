package auth

import (
	"context"
	"log/slog"
	"strings"
	"time"

	sessionauth "github.com/Optikk-Org/optikk-backend/internal/infra/session"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
	contracts "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	"golang.org/x/crypto/bcrypt"
)

type Service struct {
	repo     Repository
	sessions sessionauth.Manager
}

func NewService(repo Repository, sessions sessionauth.Manager) *Service {
	return &Service{
		repo:     repo,
		sessions: sessions,
	}
}

func (s *Service) Login(ctx context.Context, req LoginRequest, clientIP string) (AuthContextResponse, error) {
	email := strings.TrimSpace(req.Email)
	password := strings.TrimSpace(req.Password)

	user, err := s.repo.FindActiveUserByEmail(email)
	if err != nil {
		return AuthContextResponse{}, usershared.NewValidationError("Invalid email or password", err)
	}

	if user.PasswordHash != nil && *user.PasswordHash != "" && bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(password)) != nil {
		return AuthContextResponse{}, usershared.NewValidationError("Invalid email or password", nil)
	}

	if err := s.repo.UpdateUserLastLogin(user.ID, time.Now().UTC()); err != nil {
		slog.WarnContext(ctx, "AUTH_EVENT login_update_failed", slog.Int64("user_id", user.ID), slog.String("email", user.Email), slog.Any("error", err))
	}

	response, teamID, teamIDs, err := s.buildAuthContextResponse(user)
	if err != nil {
		return AuthContextResponse{}, err
	}

	if err := s.sessions.CreateAuthSession(ctx, sessionauth.AuthState{
		UserID:        user.ID,
		Email:         user.Email,
		Role:          "member",
		DefaultTeamID: teamID,
		TeamIDs:       teamIDs,
	}); err != nil {
		slog.WarnContext(ctx, "AUTH_EVENT session_create_failed", slog.Int64("user_id", user.ID), slog.String("email", user.Email), slog.Any("error", err))
		return AuthContextResponse{}, usershared.NewInternalError("Failed to create session", err)
	}

	slog.InfoContext(ctx, "AUTH_EVENT login_success", slog.Int64("user_id", user.ID), slog.String("email", user.Email), slog.Int64("team_id", teamID), slog.String("ip", clientIP))
	return response, nil
}

func (s *Service) Logout(ctx context.Context, tenant contracts.TenantContext, clientIP string) (MessageResponse, error) {
	if err := s.sessions.DestroySession(ctx); err != nil {
		return MessageResponse{}, usershared.NewInternalError("Failed to end session", err)
	}
	if tenant.UserID > 0 {
		slog.InfoContext(ctx, "AUTH_EVENT logout", slog.Int64("user_id", tenant.UserID), slog.String("email", tenant.UserEmail), slog.String("ip", clientIP))
	}
	return MessageResponse{Message: "Logged out successfully"}, nil
}

func (s *Service) AuthContext(userID int64) (AuthContextResponse, error) {
	if userID == 0 {
		return AuthContextResponse{}, usershared.NewUnauthorizedError("Not authenticated", nil)
	}

	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return AuthContextResponse{}, usershared.NewUnauthorizedError("Not authenticated", err)
	}

	authUser := usershared.AuthUser{
		ID:        user.ID,
		Email:     user.Email,
		Name:      user.Name,
		AvatarURL: user.AvatarURL,
		TeamsJSON: user.TeamsJSON,
	}
	response, _, _, err := s.buildAuthContextResponse(authUser)
	if err != nil {
		return AuthContextResponse{}, usershared.NewUnauthorizedError("Not authenticated: no teams associated", err)
	}
	return response, nil
}

func (s *Service) ValidateToken(tenant contracts.TenantContext) (ValidateTokenResponse, error) {
	if tenant.UserID == 0 {
		return ValidateTokenResponse{}, usershared.NewUnauthorizedError("Invalid or expired session", nil)
	}
	return ValidateTokenResponse{
		Valid:  true,
		UserID: tenant.UserID,
		TeamID: tenant.TeamID,
		Role:   tenant.UserRole,
	}, nil
}

func (s *Service) ForgotPassword() MessageResponse {
	return MessageResponse{
		Message: "Password resets are managed by your IT administrator. Please contact your IT admin for assistance.",
	}
}

func (s *Service) buildAuthContextResponse(user usershared.AuthUser) (resp AuthContextResponse, teamID int64, teamIDs []int64, err error) {
	teams, err := s.listTeamsForUser(user.TeamsJSON)
	if err != nil {
		slog.Warn("AUTH_EVENT team_fetch_failed", slog.Int64("user_id", user.ID), slog.String("email", user.Email), slog.Any("error", err))
		teams = []AuthTeamSummary{}
	}

	if len(teams) == 0 {
		return AuthContextResponse{}, 0, nil, usershared.NewValidationError("Account has no associated teams. Contact your administrator.", nil)
	}

	var currentTeam *AuthTeamSummary
	if len(teams) > 0 {
		currentTeam = &teams[0]
	}

	teamIDs = make([]int64, 0, len(teams))
	for _, team := range teams {
		teamIDs = append(teamIDs, team.ID)
	}

	if currentTeam != nil {
		teamID = currentTeam.ID
	}

	return AuthContextResponse{
		User: AuthUserSummary{
			ID:        user.ID,
			Email:     user.Email,
			Name:      user.Name,
			AvatarURL: user.AvatarURL,
		},
		Teams:       teams,
		CurrentTeam: currentTeam,
	}, teamID, teamIDs, nil
}

func (s *Service) listTeamsForUser(teamsJSON *string) ([]AuthTeamSummary, error) {
	memberships, err := usershared.ParseTeamMemberships(usershared.ValueOr(teamsJSON, "[]"))
	if err != nil {
		return nil, err
	}

	teamIDs := usershared.TeamIDsFromMemberships(memberships)
	if len(teamIDs) == 0 {
		return []AuthTeamSummary{}, nil
	}

	roleByTeamID := make(map[int64]string, len(memberships))
	for _, membership := range memberships {
		roleByTeamID[membership.TeamID] = membership.Role
	}

	teams, err := s.repo.ListActiveTeamsByIDs(teamIDs)
	if err != nil {
		return nil, err
	}

	items := make([]AuthTeamSummary, 0, len(teams))
	for _, team := range teams {
		items = append(items, AuthTeamSummary{
			ID:      team.ID,
			Name:    team.Name,
			Slug:    team.Slug,
			Color:   team.Color,
			OrgName: team.OrgName,
			Role:    roleByTeamID[team.ID],
		})
	}
	return items, nil
}
