package impl

import (
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	serviceinterfaces "github.com/observability/observability-backend-go/modules/user/service/interfaces"
	"github.com/observability/observability-backend-go/modules/user/store"
	"golang.org/x/crypto/bcrypt"
)

type userService struct {
	tables store.TableProvider
}

func NewUserService(tables store.TableProvider) serviceinterfaces.UserService {
	return &userService{tables: tables}
}

func (s *userService) GetCurrentUser(userID int64) (map[string]any, error) {
	user, err := buildUserResponseByID(s.tables, userID)
	if err != nil {
		return nil, newNotFoundError("User not found", err)
	}
	return user, nil
}

func (s *userService) GetUsers(organizationID int64, limit, offset int) ([]map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	users, err := s.tables.Users().ListActiveByOrganization(organizationID, limit, offset)
	if err != nil {
		return nil, newInternalError("Failed to load users", err)
	}

	userIDs := make([]int64, 0, len(users))
	for _, u := range users {
		if id := dbutil.Int64FromAny(u["id"]); id > 0 {
			userIDs = append(userIDs, id)
		}
	}

	memberships, err := s.tables.UserTeams().ListByUsers(userIDs, true)
	if err != nil {
		return nil, newInternalError("Failed to load users", err)
	}

	teamByUser := make(map[int64][]map[string]any, len(userIDs))
	for _, m := range memberships {
		uid := dbutil.Int64FromAny(m["user_id"])
		teamID := dbutil.Int64FromAny(m["team_id"])
		if uid == 0 || teamID == 0 {
			continue
		}
		teamByUser[uid] = append(teamByUser[uid], map[string]any{
			"teamId":   teamID,
			"teamName": dbutil.StringFromAny(m["team_name"]),
			"teamSlug": dbutil.StringFromAny(m["team_slug"]),
			"role":     dbutil.StringFromAny(m["role"]),
		})
	}

	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		uid := dbutil.Int64FromAny(u["id"])
		userTeams := teamByUser[uid]
		if userTeams == nil {
			userTeams = []map[string]any{}
		}
		out = append(out, map[string]any{
			"id":             uid,
			"organizationId": dbutil.Int64FromAny(u["organization_id"]),
			"email":          dbutil.StringFromAny(u["email"]),
			"name":           dbutil.StringFromAny(u["name"]),
			"avatarUrl":      dbutil.StringFromAny(u["avatar_url"]),
			"role":           dbutil.StringFromAny(u["role"]),
			"active":         dbutil.BoolFromAny(u["active"]),
			"lastLoginAt":    u["last_login_at"],
			"createdAt":      u["created_at"],
			"teams":          userTeams,
		})
	}

	return out, nil
}

func (s *userService) GetUserByID(userID int64) (map[string]any, error) {
	user, err := buildUserResponseByID(s.tables, userID)
	if err != nil {
		return nil, newNotFoundError("User not found", err)
	}
	return user, nil
}

func (s *userService) CreateUser(input serviceinterfaces.CreateUserInput) (map[string]any, error) {
	email := strings.TrimSpace(input.Email)
	name := strings.TrimSpace(input.Name)
	if email == "" || name == "" {
		return nil, newValidationError("email and name are required", nil)
	}

	role := strings.TrimSpace(input.Role)
	if role == "" {
		role = "member"
	}

	passwordHash := ""
	if input.Password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
		if err != nil {
			return nil, newInternalError("Failed to hash password", err)
		}
		passwordHash = string(hash)
	}

	userID, err := s.tables.Users().Create(input.OrganizationID, email, passwordHash, name, role, time.Now().UTC())
	if err != nil {
		return nil, newValidationError("Unable to create user", err)
	}

	teams, _ := s.tables.Teams().ListActiveByOrganization(input.OrganizationID)
	if len(teams) > 0 {
		teamID := dbutil.Int64FromAny(teams[0]["id"])
		_ = s.tables.UserTeams().Upsert(userID, teamID, role, time.Now().UTC())
	}

	user, err := buildUserResponseByID(s.tables, userID)
	if err != nil {
		return nil, newInternalError("Failed to load created user", err)
	}
	return user, nil
}

func (s *userService) Signup(input serviceinterfaces.SignupInput) (map[string]any, error) {
	email := strings.TrimSpace(input.Email)
	password := strings.TrimSpace(input.Password)
	teamName := strings.TrimSpace(input.TeamName)
	if email == "" || password == "" || teamName == "" {
		return nil, newValidationError("email, password, and teamName are required", nil)
	}

	orgID := input.TenantOrganization
	if input.OrganizationID != nil {
		orgID = *input.OrganizationID
	}
	if orgID == 0 {
		orgID = 1
	}

	slug := strings.ToLower(strings.ReplaceAll(teamName, " ", "-"))
	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, newInternalError("Failed to generate api key", err)
	}

	teamID, err := s.tables.Teams().Create(orgID, teamName, slug, nil, "#3B82F6", apiKey, time.Now().UTC())
	if err != nil {
		existingTeam, findErr := s.tables.Teams().FindBySlug(orgID, slug)
		if findErr != nil || len(existingTeam) == 0 {
			return nil, newValidationError("Unable to create team", err)
		}
		teamID = dbutil.Int64FromAny(existingTeam["id"])
		apiKey = dbutil.StringFromAny(existingTeam["api_key"])
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, newInternalError("Failed to hash password", err)
	}

	userID, err := s.tables.Users().Create(orgID, email, string(hash), strings.TrimSpace(input.Name), "admin", time.Now().UTC())
	if err != nil {
		return nil, newValidationError("Unable to create user (may already exist)", err)
	}

	_ = s.tables.UserTeams().Upsert(userID, teamID, "admin", time.Now().UTC())

	return map[string]any{
		"user_id": userID,
		"email":   email,
		"team_id": teamID,
		"api_key": apiKey,
	}, nil
}

func (s *userService) AddUserToTeam(userID, teamID int64, role string) error {
	role = strings.TrimSpace(role)
	if role == "" {
		role = "member"
	}

	if err := s.tables.UserTeams().Upsert(userID, teamID, role, time.Now().UTC()); err != nil {
		return newInternalError("Unable to add user to team", err)
	}
	return nil
}

func (s *userService) RemoveUserFromTeam(userID, teamID int64) error {
	if err := s.tables.UserTeams().Delete(userID, teamID); err != nil {
		return newInternalError("Unable to remove user from team", err)
	}
	return nil
}

func (s *userService) GetTeams(organizationID int64) ([]map[string]any, error) {
	rows, err := s.tables.Teams().ListActiveByOrganization(organizationID)
	if err != nil {
		return nil, newInternalError("Failed to load teams", err)
	}
	return dbutil.NormalizeRows(rows), nil
}

func (s *userService) GetMyTeams(userID int64) ([]map[string]any, error) {
	rows, err := s.tables.Teams().ListActiveByUser(userID)
	if err != nil {
		return nil, newInternalError("Failed to load teams", err)
	}
	return dbutil.NormalizeRows(rows), nil
}

func (s *userService) GetTeamByID(teamID int64) (map[string]any, error) {
	team, err := s.tables.Teams().FindByID(teamID)
	if err != nil || len(team) == 0 {
		return nil, newNotFoundError("Team not found", err)
	}
	return team, nil
}

func (s *userService) GetTeamBySlug(organizationID int64, slug string) (map[string]any, error) {
	team, err := s.tables.Teams().FindBySlug(organizationID, slug)
	if err != nil || len(team) == 0 {
		return nil, newNotFoundError("Team not found", err)
	}
	return team, nil
}

func (s *userService) CreateTeam(input serviceinterfaces.CreateTeamInput) (map[string]any, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return nil, newValidationError("name is required", nil)
	}

	slug := strings.TrimSpace(input.Slug)
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	}

	color := strings.TrimSpace(input.Color)
	if color == "" {
		color = "#3B82F6"
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, newInternalError("Failed to generate api key", err)
	}

	var descriptionPtr *string
	if desc := strings.TrimSpace(input.Description); desc != "" {
		descriptionPtr = &desc
	}

	teamID, err := s.tables.Teams().Create(input.OrganizationID, name, slug, descriptionPtr, color, apiKey, time.Now().UTC())
	if err != nil {
		return nil, newValidationError("Unable to create team", err)
	}

	team, err := s.tables.Teams().FindByID(teamID)
	if err != nil {
		return nil, newInternalError("Failed to load created team", err)
	}
	return team, nil
}

func (s *userService) GetProfile(userID int64) (map[string]any, error) {
	profile, err := buildSettingsResponse(s.tables, userID)
	if err != nil {
		return nil, newNotFoundError("User not found", err)
	}
	return profile, nil
}

func (s *userService) UpdateProfile(input serviceinterfaces.UpdateProfileInput) (map[string]any, error) {
	name := strings.TrimSpace(input.Name)
	avatarURL := strings.TrimSpace(input.AvatarURL)

	var namePtr *string
	var avatarPtr *string
	if name != "" {
		namePtr = &name
	}
	if avatarURL != "" {
		avatarPtr = &avatarURL
	}

	if err := s.tables.Users().UpdateProfile(input.UserID, namePtr, avatarPtr, time.Now().UTC()); err != nil {
		return nil, newInternalError("Unable to update profile", err)
	}

	profile, err := buildSettingsResponse(s.tables, input.UserID)
	if err != nil {
		return nil, newInternalError("Failed to load updated profile", err)
	}
	return profile, nil
}
