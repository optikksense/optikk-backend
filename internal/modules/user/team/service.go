package team

import (
	"strings"
	"time"

	configdefaults "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
	"go.uber.org/zap"
)

type Service struct {
	repo     Repository
	registry *configdefaults.Registry
}

func NewService(repo Repository, registry *configdefaults.Registry) *Service {
	return &Service{
		repo:     repo,
		registry: registry,
	}
}

func (s *Service) GetTeams(teamID int64) ([]TeamResponse, error) {
	currentTeam, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load team", err)
	}

	teams, err := s.repo.ListActiveTeamsByOrganization(currentTeam.OrgName)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load teams", err)
	}

	items := make([]TeamResponse, 0, len(teams))
	for _, team := range teams {
		items = append(items, toTeamResponse(team))
	}
	return items, nil
}

func (s *Service) GetMyTeams(userID int64) ([]TeamSummary, error) {
	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load teams", err)
	}

	memberships, _ := usershared.ParseTeamMemberships(user.TeamsJSON)
	teamIDs := usershared.TeamIDsFromMemberships(memberships)
	if len(teamIDs) == 0 {
		return []TeamSummary{}, nil
	}

	roleByTeamID := make(map[int64]string, len(memberships))
	for _, membership := range memberships {
		roleByTeamID[membership.TeamID] = membership.Role
	}

	teams, err := s.repo.ListActiveTeamsByIDs(teamIDs)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load teams", err)
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

func (s *Service) GetTeamByID(teamID int64) (TeamResponse, error) {
	team, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return TeamResponse{}, usershared.NewNotFoundError("Team not found", err)
	}
	return toTeamResponse(team), nil
}

func (s *Service) GetTeamBySlug(currentTeamID int64, slug string) (TeamResponse, error) {
	currentTeam, err := s.repo.FindTeamByID(currentTeamID)
	if err != nil {
		return TeamResponse{}, usershared.NewInternalError("Failed to load team", err)
	}

	team, err := s.repo.FindTeamBySlug(currentTeam.OrgName, slug)
	if err != nil {
		return TeamResponse{}, usershared.NewNotFoundError("Team not found", err)
	}
	return toTeamResponse(team), nil
}

func (s *Service) CreateTeam(req CreateTeamRequest) (TeamResponse, error) {
	name := strings.TrimSpace(req.TeamName)
	orgName := strings.TrimSpace(req.OrgName)
	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	}

	color := strings.TrimSpace(req.Color)
	if color == "" {
		color = "#3B82F6"
	}

	apiKey, err := usershared.GenerateAPIKey()
	if err != nil {
		return TeamResponse{}, usershared.NewInternalError("Failed to generate api key", err)
	}

	var descriptionPtr *string
	if description := strings.TrimSpace(req.Description); description != "" {
		descriptionPtr = &description
	}

	defaultConfigJSON := "{}"
	if s.registry != nil {
		if jsonStr, err := s.registry.GenerateDefaultDashboardConfigsJSON(); err == nil {
			defaultConfigJSON = jsonStr
		}
	}

	teamID, err := s.repo.CreateTeam(orgName, name, slug, descriptionPtr, color, apiKey, &defaultConfigJSON, time.Now().UTC())
	if err != nil {
		logger.L().Error("Failed to create team", zap.Error(err), zap.String("org_name", orgName), zap.String("name", name))
		if strings.Contains(err.Error(), "1062") || strings.Contains(err.Error(), "Duplicate entry") {
			return TeamResponse{}, usershared.NewValidationError("Team already exists in this organization", err)
		}
		return TeamResponse{}, usershared.NewInternalError("Failed to create team", err)
	}

	team, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return TeamResponse{}, usershared.NewInternalError("Failed to load created team", err)
	}
	return toTeamResponse(team), nil
}

func (s *Service) AddUserToTeam(userID, teamID int64, role string) error {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return usershared.NewInternalError("User not found", err)
	}

	memberships, _ := usershared.ParseTeamMemberships(user.TeamsJSON)
	found := false
	for i, membership := range memberships {
		if membership.TeamID == teamID {
			memberships[i].Role = role
			found = true
			break
		}
	}
	if !found {
		memberships = append(memberships, usershared.TeamMembership{TeamID: teamID, Role: role})
	}

	teamsJSON, err := usershared.BuildTeamMembershipsJSON(memberships)
	if err != nil {
		return usershared.NewInternalError("Failed to build teams JSON", err)
	}
	if err := s.repo.UpdateUserTeams(userID, teamsJSON); err != nil {
		return usershared.NewInternalError("Unable to add user to team", err)
	}
	return nil
}

func (s *Service) RemoveUserFromTeam(userID, teamID int64) error {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return usershared.NewInternalError("User not found", err)
	}

	memberships, _ := usershared.ParseTeamMemberships(user.TeamsJSON)
	filtered := make([]usershared.TeamMembership, 0, len(memberships))
	for _, membership := range memberships {
		if membership.TeamID != teamID {
			filtered = append(filtered, membership)
		}
	}

	teamsJSON, err := usershared.BuildTeamMembershipsJSON(filtered)
	if err != nil {
		return usershared.NewInternalError("Failed to build teams JSON", err)
	}
	if err := s.repo.UpdateUserTeams(userID, teamsJSON); err != nil {
		return usershared.NewInternalError("Unable to remove user from team", err)
	}
	return nil
}

func toTeamResponse(team usershared.TeamRecord) TeamResponse {
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
