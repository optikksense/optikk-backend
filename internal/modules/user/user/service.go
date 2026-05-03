package userpage

import (
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	usershared "github.com/Optikk-Org/optikk-backend/internal/modules/user/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetUsers(teamID int64, limit, offset int) ([]UserListItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	currentTeam, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load team", err)
	}

	orgTeams, err := s.repo.ListActiveTeamsByOrganization(currentTeam.OrgName)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load teams", err)
	}

	allTeamIDs := make([]int64, 0, len(orgTeams))
	for _, team := range orgTeams {
		if team.ID > 0 {
			allTeamIDs = append(allTeamIDs, team.ID)
		}
	}

	users, err := s.repo.ListActiveUsersByTeamIDs(allTeamIDs, limit, offset)
	if err != nil {
		return nil, usershared.NewInternalError("Failed to load users", err)
	}

	items := make([]UserListItem, 0, len(users))
	for _, user := range users {
		memberships, _ := usershared.ParseTeamMemberships(usershared.ValueOr(user.TeamsJSON, "[]"))
		userTeams := make([]UserListMembership, 0, len(memberships))
		for _, membership := range memberships {
			userTeams = append(userTeams, UserListMembership{
				TeamID: membership.TeamID,
				Role:   membership.Role,
			})
		}
		items = append(items, UserListItem{
			ID:          user.ID,
			Email:       user.Email,
			Name:        user.Name,
			AvatarURL:   user.AvatarURL,
			Active:      user.Active,
			LastLoginAt: user.LastLoginAt,
			CreatedAt:   user.CreatedAt,
			Teams:       userTeams,
		})
	}

	return items, nil
}

func (s *Service) GetUserByID(userID int64) (UserResponse, error) {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return UserResponse{}, usershared.NewNotFoundError("User not found", err)
	}
	return s.buildUserResponse(user)
}

func (s *Service) CreateUser(req CreateUserRequest) (UserResponse, error) {
	email := strings.TrimSpace(req.Email)
	name := strings.TrimSpace(req.Name)
	password := strings.TrimSpace(req.Password)
	role := strings.TrimSpace(req.Role)
	if role == "" {
		role = "member"
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return UserResponse{}, usershared.NewInternalError("Failed to hash password", err)
	}

	memberships := make([]usershared.TeamMembership, 0, len(req.TeamIDs))
	for _, teamID := range req.TeamIDs {
		memberships = append(memberships, usershared.TeamMembership{TeamID: teamID, Role: role})
	}
	teamsJSON, err := usershared.BuildTeamMembershipsJSON(memberships)
	if err != nil {
		return UserResponse{}, usershared.NewInternalError("Failed to build teams JSON", err)
	}

	teamsJSONPtr := &teamsJSON
	userID, err := s.repo.CreateUser(email, string(hash), name, req.AvatarURL, teamsJSONPtr, time.Now().UTC())
	if err != nil {
		return UserResponse{}, usershared.NewValidationError("Unable to create user", err)
	}

	created, err := s.repo.FindUserByID(userID)
	if err != nil {
		return UserResponse{}, usershared.NewInternalError("Failed to load created user", err)
	}
	return s.buildUserResponse(created)
}

func (s *Service) GetProfile(userID int64) (ProfileResponse, error) {
	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return ProfileResponse{}, usershared.NewNotFoundError("User not found", err)
	}
	return s.buildProfileResponse(user)
}

func (s *Service) UpdateProfile(userID int64, req UpdateProfileRequest) (ProfileResponse, error) {
	name := strings.TrimSpace(req.Name)
	avatarURL := strings.TrimSpace(req.AvatarURL)

	var namePtr *string
	var avatarPtr *string
	if name != "" {
		namePtr = &name
	}
	if avatarURL != "" {
		avatarPtr = &avatarURL
	}

	if err := s.repo.UpdateUserProfile(userID, namePtr, avatarPtr); err != nil {
		return ProfileResponse{}, usershared.NewInternalError("Unable to update profile", err)
	}

	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return ProfileResponse{}, usershared.NewInternalError("Failed to load updated profile", err)
	}
	return s.buildProfileResponse(user)
}

func (s *Service) UpdatePreferences(userID int64, req UpdatePreferencesRequest) (PreferencesResponse, error) {
	if _, err := s.repo.FindActiveUserByID(userID); err != nil {
		return PreferencesResponse{}, usershared.NewNotFoundError("User not found", err)
	}

	return PreferencesResponse(req), nil
}

func (s *Service) buildUserResponse(user usershared.UserRecord) (UserResponse, error) {
	memberships, _ := usershared.ParseTeamMemberships(usershared.ValueOr(user.TeamsJSON, "[]"))
	teamIDs := usershared.TeamIDsFromMemberships(memberships)
	roleByTeamID := make(map[int64]string, len(memberships))
	for _, membership := range memberships {
		roleByTeamID[membership.TeamID] = membership.Role
	}

	teams := make([]UserTeamDetail, 0)
	if len(teamIDs) > 0 {
		records, err := s.repo.ListActiveTeamsByIDs(teamIDs)
		if err != nil {
			return UserResponse{}, usershared.NewInternalError("Failed to load teams", err)
		}
		teams = make([]UserTeamDetail, 0, len(records))
		for _, record := range records {
			teams = append(teams, UserTeamDetail{
				TeamID:   record.ID,
				TeamName: record.Name,
				TeamSlug: record.Slug,
				Role:     roleByTeamID[record.ID],
			})
		}
	}

	return UserResponse{
		ID:          user.ID,
		Email:       user.Email,
		Name:        user.Name,
		AvatarURL:   user.AvatarURL,
		Active:      user.Active,
		LastLoginAt: user.LastLoginAt,
		CreatedAt:   user.CreatedAt,
		Teams:       teams,
	}, nil
}

func (s *Service) buildProfileResponse(user usershared.UserRecord) (ProfileResponse, error) {
	memberships, _ := usershared.ParseTeamMemberships(usershared.ValueOr(user.TeamsJSON, "[]"))
	teamIDs := usershared.TeamIDsFromMemberships(memberships)

	teams := make([]ProfileTeam, 0)
	if len(teamIDs) > 0 {
		records, err := s.repo.ListActiveTeamsByIDs(teamIDs)
		if err != nil {
			return ProfileResponse{}, usershared.NewInternalError("Failed to load teams", err)
		}
		teams = make([]ProfileTeam, 0, len(records))
		for _, record := range records {
			teams = append(teams, ProfileTeam{
				ID:          record.ID,
				OrgName:     record.OrgName,
				Name:        record.Name,
				Slug:        record.Slug,
				Description: record.Description,
				Active:      record.Active,
				Color:       record.Color,
				Icon:        record.Icon,
				APIKey:      record.APIKey,
				CreatedAt:   record.CreatedAt,
			})
		}
	}

	return ProfileResponse{
		UserID:      user.ID,
		Name:        user.Name,
		Email:       user.Email,
		AvatarURL:   user.AvatarURL,
		Preferences: defaultUserPreferences(),
		Teams:       teams,
	}, nil
}
