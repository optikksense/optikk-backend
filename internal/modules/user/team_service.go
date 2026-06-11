package user

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// GetTeams returns active teams in the same organization.
func (s *Service) GetTeams(teamID int64) ([]TeamResponse, error) {
	currentTeam, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return nil, NewInternalError("Failed to load team", err)
	}

	teams, err := s.repo.ListActiveTeamsByOrganization(currentTeam.OrgName)
	if err != nil {
		return nil, NewInternalError("Failed to load teams", err)
	}

	items := make([]TeamResponse, 0, len(teams))
	for _, team := range teams {
		items = append(items, toTeamResponse(team))
	}
	return items, nil
}

// GetMyTeams returns all teams associated with the given user.
func (s *Service) GetMyTeams(userID int64) ([]TeamSummary, error) {
	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return nil, NewInternalError("Failed to load teams", err)
	}

	memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
	teamIDs := TeamIDsFromMemberships(memberships)
	if len(teamIDs) == 0 {
		return []TeamSummary{}, nil
	}

	roleByTeamID := make(map[int64]string, len(memberships))
	for _, membership := range memberships {
		roleByTeamID[membership.TeamID] = membership.Role
	}

	teams, err := s.repo.ListActiveTeamsByIDs(teamIDs)
	if err != nil {
		return nil, NewInternalError("Failed to load teams", err)
	}

	items := make([]TeamSummary, 0, len(teams))
	for _, team := range teams {
		items = append(items, TeamSummary{
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

// GetTeamByID retrieves a team by its ID.
func (s *Service) GetTeamByID(teamID int64) (TeamResponse, error) {
	team, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return TeamResponse{}, NewNotFoundError("Team not found", err)
	}
	return toTeamResponse(team), nil
}

// GetTeamBySlug retrieves a team by its slug.
func (s *Service) GetTeamBySlug(currentTeamID int64, slug string) (TeamResponse, error) {
	currentTeam, err := s.repo.FindTeamByID(currentTeamID)
	if err != nil {
		return TeamResponse{}, NewInternalError("Failed to load team", err)
	}

	team, err := s.repo.FindTeamBySlug(currentTeam.OrgName, slug)
	if err != nil {
		return TeamResponse{}, NewNotFoundError("Team not found", err)
	}
	return toTeamResponse(team), nil
}

// CreateTeam inserts a new team with an API key.
func (s *Service) CreateTeam(req CreateTeamRequest) (TeamResponse, error) {
	name := strings.TrimSpace(req.TeamName)
	orgName := strings.TrimSpace(req.OrgName)
	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
		if len(slug) > 8 {
			slug = strings.TrimRight(slug[:8], "-")
		}
	} else if len(slug) > 50 {
		slug = strings.TrimRight(slug[:50], "-")
	}

	color := strings.TrimSpace(req.Color)
	if color == "" {
		color = "#3B82F6"
	}

	apiKey, err := GenerateAPIKey()
	if err != nil {
		return TeamResponse{}, NewInternalError("Failed to generate api key", err)
	}

	var descriptionPtr *string
	if description := strings.TrimSpace(req.Description); description != "" {
		descriptionPtr = &description
	}

	var iconPtr *string
	if icon := strings.TrimSpace(req.Icon); icon != "" {
		iconPtr = &icon
	}

	teamID, err := s.repo.CreateTeam(orgName, name, slug, descriptionPtr, iconPtr, color, apiKey, time.Now().UTC())
	if err != nil {
		slog.Error("Failed to create team", slog.Any("error", err), slog.String("org_name", orgName), slog.String("name", name))
		if strings.Contains(err.Error(), "1062") || strings.Contains(err.Error(), "Duplicate entry") {
			return TeamResponse{}, NewValidationError("Team already exists in this organization", err)
		}
		return TeamResponse{}, NewInternalError("Failed to create team", err)
	}

	team, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return TeamResponse{}, NewInternalError("Failed to load created team", err)
	}
	return toTeamResponse(team), nil
}

// AddUserToTeam adds a user to a team with a specific role.
func (s *Service) AddUserToTeam(userID, teamID int64, role string) error {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return NewInternalError("User not found", err)
	}

	memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
	found := false
	for i, membership := range memberships {
		if membership.TeamID == teamID {
			memberships[i].Role = role
			found = true
			break
		}
	}
	if !found {
		memberships = append(memberships, TeamMembership{TeamID: teamID, Role: role})
	}

	teamsJSON, err := BuildTeamMembershipsJSON(memberships)
	if err != nil {
		return NewInternalError("Failed to build teams JSON", err)
	}
	if err := s.repo.UpdateUserTeams(userID, teamsJSON); err != nil {
		return NewInternalError("Unable to add user to team", err)
	}
	return nil
}

// RemoveUserFromTeam removes a user from a team.
func (s *Service) RemoveUserFromTeam(userID, teamID int64) error {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return NewInternalError("User not found", err)
	}

	memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
	filtered := make([]TeamMembership, 0, len(memberships))
	for _, membership := range memberships {
		if membership.TeamID != teamID {
			filtered = append(filtered, membership)
		}
	}

	teamsJSON, err := BuildTeamMembershipsJSON(filtered)
	if err != nil {
		return NewInternalError("Failed to build teams JSON", err)
	}
	if err := s.repo.UpdateUserTeams(userID, teamsJSON); err != nil {
		return NewInternalError("Unable to remove user from team", err)
	}
	return nil
}

// FindTeamIDByAPIKey resolves a team ID from its API key.
func (s *Service) FindTeamIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	return s.repo.FindTeamIDByAPIKey(ctx, apiKey)
}

func toTeamResponse(team TeamRecord) TeamResponse {
	return TeamResponse{
		ID:          team.ID,
		OrgName:     team.OrgName,
		Name:        team.Name,
		Slug:        team.Slug,
		Description: team.Description,
		Active:      team.Active,
		Color:       team.Color,
		Icon:        team.Icon,
		APIKey:      team.APIKey,
		CreatedAt:   team.CreatedAt,
	}
}
