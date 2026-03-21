package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v60/github"
	contracts "github.com/observability/observability-backend-go/internal/contracts"
	usershared "github.com/observability/observability-backend-go/internal/modules/user/internal/shared"
	sessionauth "github.com/observability/observability-backend-go/internal/platform/session"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
	googleOAuth2 "golang.org/x/oauth2/google"
)

type Service struct {
	repo         Repository
	sessions     *sessionauth.Manager
	googleConfig *oauth2.Config
	githubConfig *oauth2.Config
	redirectBase string
}

func NewService(
	repo Repository,
	sessions *sessionauth.Manager,
	googleClientID, googleClientSecret,
	githubClientID, githubClientSecret,
	redirectBase string,
) *Service {
	service := &Service{
		repo:         repo,
		sessions:     sessions,
		redirectBase: strings.TrimRight(redirectBase, "/"),
	}

	if googleClientID != "" && googleClientSecret != "" {
		service.googleConfig = &oauth2.Config{
			ClientID:     googleClientID,
			ClientSecret: googleClientSecret,
			RedirectURL:  redirectBase + "/api/v1/auth/google/callback",
			Scopes:       []string{"openid", "email", "profile"},
			Endpoint:     googleOAuth2.Endpoint,
		}
	}

	if githubClientID != "" && githubClientSecret != "" {
		service.githubConfig = &oauth2.Config{
			ClientID:     githubClientID,
			ClientSecret: githubClientSecret,
			RedirectURL:  redirectBase + "/api/v1/auth/github/callback",
			Scopes:       []string{"user:email", "read:user"},
			Endpoint: oauth2.Endpoint{
				AuthURL:  "https://github.com/login/oauth/authorize",
				TokenURL: "https://github.com/login/oauth/access_token",
			},
		}
	}

	return service
}

func (s *Service) GoogleConfigured() bool {
	return s.googleConfig != nil
}

func (s *Service) GithubConfigured() bool {
	return s.githubConfig != nil
}

func (s *Service) Login(ctx context.Context, req LoginRequest, clientIP string) (AuthContextResponse, error) {
	email := strings.TrimSpace(req.Email)
	password := strings.TrimSpace(req.Password)

	user, err := s.repo.FindActiveUserByEmail(email)
	if err != nil {
		return AuthContextResponse{}, usershared.NewValidationError("Invalid email or password", err)
	}

	if user.PasswordHash != "" && bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return AuthContextResponse{}, usershared.NewValidationError("Invalid email or password", nil)
	}

	if err := s.repo.UpdateUserLastLogin(user.ID, time.Now().UTC()); err != nil {
		log.Printf("AUTH_EVENT login_update_failed user_id=%d email=%s err=%v", user.ID, user.Email, err)
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
		return AuthContextResponse{}, usershared.NewInternalError("Failed to create session", err)
	}

	log.Printf("AUTH_EVENT login_success user_id=%d email=%s team_id=%d ip=%s", user.ID, user.Email, teamID, clientIP)
	return response, nil
}

func (s *Service) Logout(ctx context.Context, tenant contracts.TenantContext, clientIP string) (MessageResponse, error) {
	if err := s.sessions.DestroySession(ctx); err != nil {
		return MessageResponse{}, usershared.NewInternalError("Failed to end session", err)
	}
	if tenant.UserID > 0 {
		log.Printf("AUTH_EVENT logout user_id=%d email=%s ip=%s", tenant.UserID, tenant.UserEmail, clientIP)
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

func (s *Service) GoogleLoginURL() (string, error) {
	if s.googleConfig == nil {
		return "", usershared.NewInternalError("Google OAuth is not configured", nil)
	}
	state, err := randomState()
	if err != nil {
		return "", usershared.NewInternalError("Failed to generate state", err)
	}
	return s.googleConfig.AuthCodeURL(state, oauth2.AccessTypeOnline), nil
}

func (s *Service) GoogleCallbackRedirect(code string) string {
	if code == "" {
		return s.redirectBase + "/login?error=oauth_cancelled"
	}

	token, err := s.googleConfig.Exchange(context.Background(), code)
	if err != nil {
		return s.redirectBase + "/login?error=oauth_failed"
	}

	client := s.googleConfig.Client(context.Background(), token)
	resp, err := client.Get("https://www.googleapis.com/oauth2/v2/userinfo")
	if err != nil || resp.StatusCode != http.StatusOK {
		return s.redirectBase + "/login?error=oauth_failed"
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return s.redirectBase + "/login?error=oauth_failed"
	}

	var info struct {
		ID      string `json:"id"`
		Email   string `json:"email"`
		Name    string `json:"name"`
		Picture string `json:"picture"`
	}
	if err := json.Unmarshal(body, &info); err != nil || info.ID == "" {
		return s.redirectBase + "/login?error=oauth_failed"
	}

	return s.handleOAuthCallback(context.Background(), "google", info.ID, info.Email, info.Name, info.Picture)
}

func (s *Service) GithubLoginURL() (string, error) {
	if s.githubConfig == nil {
		return "", usershared.NewInternalError("GitHub OAuth is not configured", nil)
	}
	state, err := randomState()
	if err != nil {
		return "", usershared.NewInternalError("Failed to generate state", err)
	}
	return s.githubConfig.AuthCodeURL(state), nil
}

func (s *Service) GithubCallbackRedirect(code string) string {
	if code == "" {
		return s.redirectBase + "/login?error=oauth_cancelled"
	}

	token, err := s.githubConfig.Exchange(context.Background(), code)
	if err != nil {
		return s.redirectBase + "/login?error=oauth_failed"
	}

	httpClient := s.githubConfig.Client(context.Background(), token)
	ghClient := github.NewClient(httpClient)

	ghUser, _, err := ghClient.Users.Get(context.Background(), "")
	if err != nil || ghUser == nil {
		return s.redirectBase + "/login?error=oauth_failed"
	}

	email := ghUser.GetEmail()
	if email == "" {
		emails, _, err := ghClient.Users.ListEmails(context.Background(), nil)
		if err == nil {
			for _, item := range emails {
				if item.GetPrimary() && item.GetVerified() {
					email = item.GetEmail()
					break
				}
			}
		}
	}
	if email == "" {
		return s.redirectBase + "/login?error=no_email"
	}

	oauthID := strconv.FormatInt(ghUser.GetID(), 10)
	name := ghUser.GetName()
	if name == "" {
		name = ghUser.GetLogin()
	}

	return s.handleOAuthCallback(context.Background(), "github", oauthID, email, name, ghUser.GetAvatarURL())
}

func (s *Service) CompleteSignup(ctx context.Context, req CompleteSignupRequest) (AuthContextResponse, error) {
	pending, ok := s.sessions.GetPendingOAuth(ctx)
	if !ok {
		return AuthContextResponse{}, usershared.NewUnauthorizedError("OAuth signup session is missing or expired", nil)
	}

	team, err := s.repo.FindTeamByOrgAndName(strings.TrimSpace(req.OrgName), strings.TrimSpace(req.TeamName))
	if err == sql.ErrNoRows {
		return AuthContextResponse{}, usershared.NewValidationError("No team found with that name and org. Contact your IT admin.", err)
	}
	if err != nil {
		return AuthContextResponse{}, usershared.NewInternalError("Failed to look up team", err)
	}

	existingUser, err := s.repo.FindActiveUserByEmail(pending.Email)
	if err == nil {
		if err := s.repo.UpdateUserOAuth(existingUser.ID, pending.Provider, pending.OAuthID); err != nil {
			return AuthContextResponse{}, usershared.NewInternalError("Failed to link OAuth account", err)
		}
		if err := s.startUserSession(ctx, existingUser); err != nil {
			return AuthContextResponse{}, err
		}
		response, _, _, err := s.buildAuthContextResponse(existingUser)
		if err != nil {
			return AuthContextResponse{}, err
		}
		return response, nil
	}

	memberships := []usershared.TeamMembership{{TeamID: team.ID, Role: "member"}}
	teamsJSON, err := usershared.BuildTeamMembershipsJSON(memberships)
	if err != nil {
		return AuthContextResponse{}, usershared.NewInternalError("Failed to build teams JSON", err)
	}

	userID, err := s.repo.CreateOAuthUser(
		pending.Email,
		pending.Name,
		pending.AvatarURL,
		teamsJSON,
		pending.Provider,
		pending.OAuthID,
		time.Now().UTC(),
	)
	if err != nil {
		return AuthContextResponse{}, usershared.NewValidationError("Unable to create user", err)
	}

	createdUser, err := s.repo.FindActiveUserByEmail(pending.Email)
	if err != nil {
		return AuthContextResponse{}, usershared.NewInternalError("Failed to load created user", err)
	}
	createdUser.ID = userID

	if err := s.startUserSession(ctx, createdUser); err != nil {
		return AuthContextResponse{}, err
	}
	response, _, _, err := s.buildAuthContextResponse(createdUser)
	if err != nil {
		return AuthContextResponse{}, err
	}
	return response, nil
}

func (s *Service) ForgotPassword() MessageResponse {
	return MessageResponse{
		Message: "Password resets are managed by your IT administrator. Please contact your IT admin for assistance.",
	}
}

func (s *Service) handleOAuthCallback(ctx context.Context, provider, oauthID, email, name, avatarURL string) string {
	user, err := s.repo.FindUserByOAuth(provider, oauthID)
	if err == nil {
		if err := s.startUserSession(ctx, user); err != nil {
			return s.redirectBase + "/login?error=session_failed"
		}
		return s.redirectBase + "/oauth/success"
	}

	existingUser, err := s.repo.FindActiveUserByEmail(email)
	if err == nil {
		if err := s.repo.UpdateUserOAuth(existingUser.ID, provider, oauthID); err != nil {
			return s.redirectBase + "/login?error=oauth_failed"
		}
		if err := s.startUserSession(ctx, existingUser); err != nil {
			return s.redirectBase + "/login?error=session_failed"
		}
		return s.redirectBase + "/oauth/success"
	}

	if err := s.sessions.SetPendingOAuth(ctx, sessionauth.PendingOAuthState{
		Provider:  provider,
		OAuthID:   oauthID,
		Email:     email,
		Name:      name,
		AvatarURL: avatarURL,
	}); err != nil {
		return s.redirectBase + "/login?error=session_failed"
	}

	return fmt.Sprintf(
		"%s/oauth/signup?name=%s&email=%s",
		s.redirectBase,
		urlEncode(name),
		urlEncode(email),
	)
}

func (s *Service) startUserSession(ctx context.Context, user usershared.AuthUser) error {
	_, teamID, teamIDs, err := s.buildAuthContextResponse(user)
	if err != nil {
		return err
	}
	_ = s.repo.UpdateUserLastLogin(user.ID, time.Now().UTC())
	return s.sessions.CreateAuthSession(ctx, sessionauth.AuthState{
		UserID:        user.ID,
		Email:         user.Email,
		Role:          "member",
		DefaultTeamID: teamID,
		TeamIDs:       teamIDs,
	})
}

func (s *Service) buildAuthContextResponse(user usershared.AuthUser) (AuthContextResponse, int64, []int64, error) {
	teams, err := s.listTeamsForUser(user.TeamsJSON)
	if err != nil {
		log.Printf("AUTH_EVENT team_fetch_failed user_id=%d email=%s err=%v", user.ID, user.Email, err)
		teams = []AuthTeamSummary{}
	}

	if len(teams) == 0 {
		return AuthContextResponse{}, 0, nil, usershared.NewValidationError("Account has no associated teams. Contact your administrator.", nil)
	}

	var currentTeam *AuthTeamSummary
	if len(teams) > 0 {
		currentTeam = &teams[0]
	}

	var teamID int64
	teamIDs := make([]int64, 0, len(teams))
	for _, team := range teams {
		teamIDs = append(teamIDs, team.ID)
	}

	if currentTeam != nil {
		teamID = currentTeam.ID
	} else {
		teamID = 0
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

func (s *Service) listTeamsForUser(teamsJSON string) ([]AuthTeamSummary, error) {
	memberships, err := usershared.ParseTeamMemberships(teamsJSON)
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

func randomState() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func urlEncode(value string) string {
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '~':
			builder.WriteRune(r)
		default:
			for _, byt := range []byte(string(r)) {
				fmt.Fprintf(&builder, "%%%02X", byt)
			}
		}
	}
	return builder.String()
}
