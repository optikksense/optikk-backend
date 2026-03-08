package user

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v60/github"
	"golang.org/x/oauth2"
	googleOAuth2 "golang.org/x/oauth2/google"

	apphandlers "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/platform/auth"
)

type OAuthHandler struct {
	store        *Store
	jwtManager   auth.JWTManager
	jwtExpiresMs int64
	googleConfig *oauth2.Config
	githubConfig *oauth2.Config
	redirectBase string
}

func NewOAuthHandler(
	store *Store,
	jwtManager auth.JWTManager,
	jwtExpiresMs int64,
	googleClientID, googleClientSecret,
	githubClientID, githubClientSecret,
	redirectBase string,
) *OAuthHandler {
	h := &OAuthHandler{
		store:        store,
		jwtManager:   jwtManager,
		jwtExpiresMs: jwtExpiresMs,
		redirectBase: strings.TrimRight(redirectBase, "/"),
	}

	if googleClientID != "" && googleClientSecret != "" {
		h.googleConfig = &oauth2.Config{
			ClientID:     googleClientID,
			ClientSecret: googleClientSecret,
			RedirectURL:  redirectBase + "/api/v1/auth/google/callback",
			Scopes:       []string{"openid", "email", "profile"},
			Endpoint:     googleOAuth2.Endpoint,
		}
	}

	if githubClientID != "" && githubClientSecret != "" {
		h.githubConfig = &oauth2.Config{
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

	return h
}

func randomState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// GoogleLogin redirects the browser to the Google OAuth consent screen.
func (h *OAuthHandler) GoogleLogin(c *gin.Context) {
	if h.googleConfig == nil {
		apphandlers.RespondError(c, http.StatusServiceUnavailable, "OAUTH_NOT_CONFIGURED", "Google OAuth is not configured")
		return
	}
	state, err := randomState()
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate state")
		return
	}
	url := h.googleConfig.AuthCodeURL(state, oauth2.AccessTypeOnline)
	c.Redirect(http.StatusTemporaryRedirect, url)
}

// GoogleCallback handles the Google OAuth callback after user consent.
func (h *OAuthHandler) GoogleCallback(c *gin.Context) {
	if h.googleConfig == nil {
		apphandlers.RespondError(c, http.StatusServiceUnavailable, "OAUTH_NOT_CONFIGURED", "Google OAuth is not configured")
		return
	}

	code := c.Query("code")
	if code == "" {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_cancelled")
		return
	}

	token, err := h.googleConfig.Exchange(context.Background(), code)
	if err != nil {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}

	client := h.googleConfig.Client(context.Background(), token)
	resp, err := client.Get("https://www.googleapis.com/oauth2/v2/userinfo")
	if err != nil || resp.StatusCode != http.StatusOK {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}

	var info struct {
		ID      string `json:"id"`
		Email   string `json:"email"`
		Name    string `json:"name"`
		Picture string `json:"picture"`
	}
	if err := json.Unmarshal(body, &info); err != nil || info.ID == "" {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}

	h.handleOAuthCallback(c, "google", info.ID, info.Email, info.Name, info.Picture)
}

// GithubLogin redirects the browser to the GitHub OAuth consent screen.
func (h *OAuthHandler) GithubLogin(c *gin.Context) {
	if h.githubConfig == nil {
		apphandlers.RespondError(c, http.StatusServiceUnavailable, "OAUTH_NOT_CONFIGURED", "GitHub OAuth is not configured")
		return
	}
	state, err := randomState()
	if err != nil {
		apphandlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate state")
		return
	}
	url := h.githubConfig.AuthCodeURL(state)
	c.Redirect(http.StatusTemporaryRedirect, url)
}

// GithubCallback handles the GitHub OAuth callback after user consent.
func (h *OAuthHandler) GithubCallback(c *gin.Context) {
	if h.githubConfig == nil {
		apphandlers.RespondError(c, http.StatusServiceUnavailable, "OAUTH_NOT_CONFIGURED", "GitHub OAuth is not configured")
		return
	}

	code := c.Query("code")
	if code == "" {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_cancelled")
		return
	}

	token, err := h.githubConfig.Exchange(context.Background(), code)
	if err != nil {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}

	httpClient := h.githubConfig.Client(context.Background(), token)
	ghClient := github.NewClient(httpClient)

	ghUser, _, err := ghClient.Users.Get(context.Background(), "")
	if err != nil || ghUser == nil {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=oauth_failed")
		return
	}

	email := ghUser.GetEmail()
	if email == "" {
		// Fetch primary email via emails endpoint if not public.
		emails, _, err := ghClient.Users.ListEmails(context.Background(), nil)
		if err == nil {
			for _, e := range emails {
				if e.GetPrimary() && e.GetVerified() {
					email = e.GetEmail()
					break
				}
			}
		}
	}
	if email == "" {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=no_email")
		return
	}

	oauthID := strconv.FormatInt(ghUser.GetID(), 10)
	name := ghUser.GetName()
	if name == "" {
		name = ghUser.GetLogin()
	}
	avatar := ghUser.GetAvatarURL()

	h.handleOAuthCallback(c, "github", oauthID, email, name, avatar)
}

// handleOAuthCallback is the shared logic after a successful OAuth provider authentication.
// It finds or creates the user and redirects to the frontend with a JWT or pending token.
func (h *OAuthHandler) handleOAuthCallback(c *gin.Context, provider, oauthID, email, name, avatarURL string) {
	// 1. Check if a user with this oauth_provider+oauth_id already exists.
	user, err := h.store.FindUserByOAuth(provider, oauthID)
	if err == nil {
		// Existing OAuth user — issue full JWT.
		jwt, redirectURL := h.issueJWTAndRedirect(user)
		if jwt == "" {
			c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=token_failed")
			return
		}
		c.Redirect(http.StatusTemporaryRedirect, redirectURL)
		return
	}

	// 2. Check if a user with this email already exists (e.g. signed up with password before).
	existingUser, err := h.store.FindActiveUserByEmail(email)
	if err == nil {
		// Link OAuth to existing account and log them in.
		_ = h.store.UpdateUserOAuth(existingUser.ID, provider, oauthID, time.Now().UTC())
		jwtToken, redirectURL := h.issueJWTAndRedirect(existingUser)
		if jwtToken == "" {
			c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=token_failed")
			return
		}
		c.Redirect(http.StatusTemporaryRedirect, redirectURL)
		return
	}

	// 3. New user — issue a short-lived pending token and redirect to signup page.
	pendingToken, err := h.jwtManager.GeneratePendingOAuthToken(provider, oauthID, email, name, avatarURL)
	if err != nil {
		c.Redirect(http.StatusTemporaryRedirect, h.redirectBase+"/login?error=token_failed")
		return
	}

	signupURL := fmt.Sprintf(
		"%s/oauth/signup?pending=%s&name=%s&email=%s",
		h.redirectBase,
		pendingToken,
		urlEncode(name),
		urlEncode(email),
	)
	c.Redirect(http.StatusTemporaryRedirect, signupURL)
}

// issueJWTAndRedirect generates a full JWT for the user and returns the token + redirect URL.
func (h *OAuthHandler) issueJWTAndRedirect(user AuthUser) (string, string) {
	teams, _ := listActiveTeamsForUser(h.store, user.ID)

	teamID := int64(0)
	var teamIDs []int64
	for _, t := range teams {
		tid := mapInt64(t, "id")
		teamIDs = append(teamIDs, tid)
		if teamID == 0 {
			teamID = tid
		}
	}
	if teamID == 0 {
		teamID = 1
	}

	_ = h.store.UpdateUserLastLogin(user.ID, time.Now().UTC())

	jwtToken, err := h.jwtManager.Generate(user.ID, user.Email, user.Name, user.Role, teamID, teamIDs...)
	if err != nil {
		return "", ""
	}

	redirectURL := fmt.Sprintf("%s/oauth/success?token=%s", h.redirectBase, jwtToken)
	return jwtToken, redirectURL
}

// CompleteSignup handles POST /api/v1/auth/oauth/complete-signup.
// Validates the pending OAuth token, checks the org+team exists, and creates the user.
func (h *OAuthHandler) CompleteSignup(c *gin.Context) {
	var req struct {
		PendingToken string `json:"pendingToken" binding:"required"`
		TeamName     string `json:"teamName" binding:"required"`
		OrgName      string `json:"orgName" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		apphandlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "pendingToken, teamName, and orgName are required")
		return
	}

	// Validate the pending token.
	claims, err := h.jwtManager.ParsePendingOAuthToken(req.PendingToken)
	if err != nil {
		apphandlers.RespondError(c, http.StatusUnauthorized, "INVALID_TOKEN", "Invalid or expired signup token")
		return
	}

	// Validate that the team+org combination exists.
	team, err := h.store.FindTeamByOrgAndName(strings.TrimSpace(req.OrgName), strings.TrimSpace(req.TeamName))
	if err == sql.ErrNoRows || len(team) == 0 {
		apphandlers.RespondError(c, http.StatusBadRequest, "TEAM_NOT_FOUND", "No team found with that name and org. Contact your IT admin.")
		return
	}
	if err != nil {
		respondServiceError(c, newInternalError("Failed to look up team", err), "Unable to complete signup")
		return
	}

	teamID := mapInt64(team, "id")

	// Guard against duplicate email (race condition).
	existingUser, err := h.store.FindActiveUserByEmail(claims.Email)
	if err == nil {
		// Already exists — just link OAuth and log them in.
		_ = h.store.UpdateUserOAuth(existingUser.ID, claims.Provider, claims.OAuthID, time.Now().UTC())
		jwtToken, _ := h.issueJWTAndRedirectJSON(c, existingUser)
		if jwtToken == "" {
			return
		}
		return
	}

	// Create the new user.
	memberships := []TeamMembership{{TeamID: teamID, Role: "member"}}
	teamsJSON, err := buildTeamsJSON(memberships)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to build teams JSON", err), "Unable to complete signup")
		return
	}

	userID, err := h.store.CreateOAuthUser(
		claims.Email, claims.Name, claims.AvatarURL, "member",
		teamsJSON, claims.Provider, claims.OAuthID, time.Now().UTC(),
	)
	if err != nil {
		respondServiceError(c, newValidationError("Unable to create user", err), "Unable to complete signup")
		return
	}

	newUser, err := h.store.FindActiveUserByEmail(claims.Email)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to load created user", err), "Unable to complete signup")
		return
	}
	newUser.ID = userID

	h.issueJWTAndRedirectJSON(c, newUser)
}

// issueJWTAndRedirectJSON issues a JWT and returns it as JSON (for complete-signup endpoint).
func (h *OAuthHandler) issueJWTAndRedirectJSON(c *gin.Context, user AuthUser) (string, []map[string]any) {
	teams, _ := listActiveTeamsForUser(h.store, user.ID)

	teamID := int64(0)
	var teamIDs []int64
	for _, t := range teams {
		tid := mapInt64(t, "id")
		teamIDs = append(teamIDs, tid)
		if teamID == 0 {
			teamID = tid
		}
	}
	if teamID == 0 {
		teamID = 1
	}

	_ = h.store.UpdateUserLastLogin(user.ID, time.Now().UTC())

	jwtToken, err := h.jwtManager.Generate(user.ID, user.Email, user.Name, user.Role, teamID, teamIDs...)
	if err != nil {
		respondServiceError(c, newInternalError("Failed to generate token", err), "Unable to complete signup")
		return "", nil
	}

	var currentTeam map[string]any
	if len(teams) > 0 {
		currentTeam = teams[0]
	}

	apphandlers.RespondOK(c, map[string]any{
		"token":       jwtToken,
		"tokenType":   "Bearer",
		"expiresIn":   h.jwtExpiresMs,
		"user":        map[string]any{"id": user.ID, "email": user.Email, "name": user.Name, "avatarUrl": user.AvatarURL, "role": user.Role},
		"teams":       teams,
		"currentTeam": currentTeam,
	})
	return jwtToken, teams
}

// ForgotPassword handles POST /api/v1/auth/forgot-password.
// Always returns a static message directing the user to contact their IT admin.
func (h *OAuthHandler) ForgotPassword(c *gin.Context) {
	apphandlers.RespondOK(c, map[string]string{
		"message": "Password resets are managed by your IT administrator. Please contact your IT admin for assistance.",
	})
}

// urlEncode percent-encodes a string for use in a URL query parameter.
func urlEncode(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.', r == '~':
			b.WriteRune(r)
		default:
			for _, byt := range []byte(string(r)) {
				fmt.Fprintf(&b, "%%%02X", byt)
			}
		}
	}
	return b.String()
}
