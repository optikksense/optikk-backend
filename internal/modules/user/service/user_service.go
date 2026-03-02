package service

import (
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/user/store"
	"golang.org/x/crypto/bcrypt"
)

type userService struct {
	tables store.TableProvider
}

func NewUserService(tables store.TableProvider) UserService {
	return &userService{tables: tables}
}

func (s *userService) GetCurrentUser(userID int64) (map[string]any, error) {
	user, err := buildUserResponseByID(s.tables, userID)
	if err != nil {
		return nil, newNotFoundError("User not found", err)
	}
	return user, nil
}

func (s *userService) GetUsers(teamID int64, limit, offset int) ([]map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	// Derive organization from the current team, then get all teams in that org.
	team, err := s.tables.Teams().FindByID(teamID)
	if err != nil {
		return nil, newInternalError("Failed to load team", err)
	}
	orgID := dbutil.Int64FromAny(team["organization_id"])

	orgTeams, err := s.tables.Teams().ListActiveByOrganization(orgID)
	if err != nil {
		return nil, newInternalError("Failed to load teams", err)
	}

	allTeamIDs := make([]int64, 0, len(orgTeams))
	for _, t := range orgTeams {
		if tid := dbutil.Int64FromAny(t["id"]); tid > 0 {
			allTeamIDs = append(allTeamIDs, tid)
		}
	}

	users, err := s.tables.Users().ListActiveByTeamIDs(allTeamIDs, limit, offset)
	if err != nil {
		return nil, newInternalError("Failed to load users", err)
	}

	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		teamsJSON := dbutil.StringFromAny(u["teams"])
		memberships, _ := parseTeamsJSON(teamsJSON)

		userTeams := make([]map[string]any, 0, len(memberships))
		for _, m := range memberships {
			userTeams = append(userTeams, map[string]any{
				"teamId": m.TeamID,
				"role":   m.Role,
			})
		}

		out = append(out, map[string]any{
			"id":          dbutil.Int64FromAny(u["id"]),
			"email":       dbutil.StringFromAny(u["email"]),
			"name":        dbutil.StringFromAny(u["name"]),
			"avatarUrl":   dbutil.StringFromAny(u["avatar_url"]),
			"role":        dbutil.StringFromAny(u["role"]),
			"active":      dbutil.BoolFromAny(u["active"]),
			"lastLoginAt": u["last_login_at"],
			"createdAt":   u["created_at"],
			"teams":       userTeams,
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

func (s *userService) CreateUser(input CreateUserInput) (map[string]any, error) {
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

	// Build teams JSON from input team IDs.
	memberships := make([]TeamMembership, 0, len(input.TeamIDs))
	for _, tid := range input.TeamIDs {
		memberships = append(memberships, TeamMembership{TeamID: tid, Role: role})
	}
	teamsJSON, err := buildTeamsJSON(memberships)
	if err != nil {
		return nil, newInternalError("Failed to build teams JSON", err)
	}

	userID, err := s.tables.Users().Create(email, passwordHash, name, role, teamsJSON, time.Now().UTC())
	if err != nil {
		return nil, newValidationError("Unable to create user", err)
	}

	user, err := buildUserResponseByID(s.tables, userID)
	if err != nil {
		return nil, newInternalError("Failed to load created user", err)
	}
	return user, nil
}

func (s *userService) Signup(input SignupInput) (map[string]any, error) {
	email := strings.TrimSpace(input.Email)
	password := strings.TrimSpace(input.Password)
	teamName := strings.TrimSpace(input.TeamName)
	if email == "" || password == "" || teamName == "" {
		return nil, newValidationError("email, password, and teamName are required", nil)
	}

	orgID := int64(1)
	orgName := strings.TrimSpace(input.OrgName)
	if orgName == "" {
		orgName = "Default"
	}

	slug := strings.ToLower(strings.ReplaceAll(teamName, " ", "-"))
	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, newInternalError("Failed to generate api key", err)
	}

	teamID, err := s.tables.Teams().Create(orgID, teamName, slug, nil, "#3B82F6", apiKey, orgName, time.Now().UTC())
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

	teamsJSON, _ := buildTeamsJSON([]TeamMembership{{TeamID: teamID, Role: "admin"}})

	userID, err := s.tables.Users().Create(email, string(hash), strings.TrimSpace(input.Name), "admin", teamsJSON, time.Now().UTC())
	if err != nil {
		return nil, newValidationError("Unable to create user (may already exist)", err)
	}

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

	user, err := s.tables.Users().FindByID(userID)
	if err != nil {
		return newInternalError("User not found", err)
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)

	// Update existing or append new membership.
	found := false
	for i, m := range memberships {
		if m.TeamID == teamID {
			memberships[i].Role = role
			found = true
			break
		}
	}
	if !found {
		memberships = append(memberships, TeamMembership{TeamID: teamID, Role: role})
	}

	newJSON, err := buildTeamsJSON(memberships)
	if err != nil {
		return newInternalError("Failed to build teams JSON", err)
	}

	if err := s.tables.Users().UpdateTeams(userID, newJSON, time.Now().UTC()); err != nil {
		return newInternalError("Unable to add user to team", err)
	}
	return nil
}

func (s *userService) RemoveUserFromTeam(userID, teamID int64) error {
	user, err := s.tables.Users().FindByID(userID)
	if err != nil {
		return newInternalError("User not found", err)
	}

	teamsJSON := dbutil.StringFromAny(user["teams"])
	memberships, _ := parseTeamsJSON(teamsJSON)

	filtered := make([]TeamMembership, 0, len(memberships))
	for _, m := range memberships {
		if m.TeamID != teamID {
			filtered = append(filtered, m)
		}
	}

	newJSON, err := buildTeamsJSON(filtered)
	if err != nil {
		return newInternalError("Failed to build teams JSON", err)
	}

	if err := s.tables.Users().UpdateTeams(userID, newJSON, time.Now().UTC()); err != nil {
		return newInternalError("Unable to remove user from team", err)
	}
	return nil
}

func (s *userService) GetTeams(teamID int64) ([]map[string]any, error) {
	// Derive organization from the current team, then list all org teams.
	team, err := s.tables.Teams().FindByID(teamID)
	if err != nil {
		return nil, newInternalError("Failed to load team", err)
	}
	orgID := dbutil.Int64FromAny(team["organization_id"])

	rows, err := s.tables.Teams().ListActiveByOrganization(orgID)
	if err != nil {
		return nil, newInternalError("Failed to load teams", err)
	}
	return dbutil.NormalizeRows(rows), nil
}

func (s *userService) GetMyTeams(userID int64) ([]map[string]any, error) {
	teams, err := listActiveTeamsForUser(s.tables, userID)
	if err != nil {
		return nil, newInternalError("Failed to load teams", err)
	}
	return teams, nil
}

func (s *userService) GetTeamByID(teamID int64) (map[string]any, error) {
	team, err := s.tables.Teams().FindByID(teamID)
	if err != nil || len(team) == 0 {
		return nil, newNotFoundError("Team not found", err)
	}
	return team, nil
}

func (s *userService) GetTeamBySlug(teamID int64, slug string) (map[string]any, error) {
	// Derive organization from the current team.
	currentTeam, err := s.tables.Teams().FindByID(teamID)
	if err != nil {
		return nil, newInternalError("Failed to load team", err)
	}
	orgID := dbutil.Int64FromAny(currentTeam["organization_id"])

	team, err := s.tables.Teams().FindBySlug(orgID, slug)
	if err != nil || len(team) == 0 {
		return nil, newNotFoundError("Team not found", err)
	}
	return team, nil
}

func (s *userService) CreateTeam(input CreateTeamInput) (map[string]any, error) {
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

	orgName := strings.TrimSpace(input.OrgName)
	if orgName == "" {
		orgName = "Default"
	}

	apiKey, err := generateAPIKey()
	if err != nil {
		return nil, newInternalError("Failed to generate api key", err)
	}

	var descriptionPtr *string
	if desc := strings.TrimSpace(input.Description); desc != "" {
		descriptionPtr = &desc
	}

	teamID, err := s.tables.Teams().Create(input.OrganizationID, name, slug, descriptionPtr, color, apiKey, orgName, time.Now().UTC())
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

func (s *userService) UpdateProfile(input UpdateProfileInput) (map[string]any, error) {
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
