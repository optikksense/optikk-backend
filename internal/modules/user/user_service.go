package user

import (
	"time"

	"golang.org/x/crypto/bcrypt"
)

// GetUsers returns a paginated list of users belonging to the org.
func (s *Service) GetUsers(teamID int64, limit, offset int) ([]UserListItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	currentTeam, err := s.repo.FindTeamByID(teamID)
	if err != nil {
		return nil, NewInternalError("Failed to load team", err)
	}

	orgTeams, err := s.repo.ListActiveTeamsByOrganization(currentTeam.OrgName)
	if err != nil {
		return nil, NewInternalError("Failed to load teams", err)
	}

	allTeamIDs := make([]int64, 0, len(orgTeams))
	for _, team := range orgTeams {
		if team.ID > 0 {
			allTeamIDs = append(allTeamIDs, team.ID)
		}
	}

	users, err := s.repo.ListActiveUsersByTeamIDs(allTeamIDs, limit, offset)
	if err != nil {
		return nil, NewInternalError("Failed to load users", err)
	}

	items := make([]UserListItem, 0, len(users))
	for _, user := range users {
		memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
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

// GetUserByID retrieves a user by ID.
func (s *Service) GetUserByID(userID int64) (UserResponse, error) {
	user, err := s.repo.FindUserByID(userID)
	if err != nil {
		return UserResponse{}, NewNotFoundError("User not found", err)
	}
	return s.buildUserResponse(user)
}

// CreateUser registers a new user with their team associations.
func (s *Service) CreateUser(req CreateUserRequest) (UserResponse, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return UserResponse{}, NewInternalError("Failed to hash password", err)
	}

	role := req.Role
	if role == "" {
		role = "member"
	}

	memberships := make([]TeamMembership, 0, len(req.TeamIDs))
	for _, teamID := range req.TeamIDs {
		memberships = append(memberships, TeamMembership{TeamID: teamID, Role: role})
	}
	teamsJSON, err := BuildTeamMembershipsJSON(memberships)
	if err != nil {
		return UserResponse{}, NewInternalError("Failed to build teams JSON", err)
	}

	teamsJSONPtr := &teamsJSON
	userID, err := s.repo.CreateUser(req.Email, string(hash), req.Name, req.AvatarURL, teamsJSONPtr, time.Now().UTC())
	if err != nil {
		return UserResponse{}, NewValidationError("Unable to create user", err)
	}

	created, err := s.repo.FindUserByID(userID)
	if err != nil {
		return UserResponse{}, NewInternalError("Failed to load created user", err)
	}
	return s.buildUserResponse(created)
}

// GetProfile loads user profile details.
func (s *Service) GetProfile(userID int64) (ProfileResponse, error) {
	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return ProfileResponse{}, NewNotFoundError("User not found", err)
	}
	return s.buildProfileResponse(user)
}

// UpdateProfile updates user profile name and avatar.
func (s *Service) UpdateProfile(userID int64, req UpdateProfileRequest) (ProfileResponse, error) {
	var namePtr *string
	var avatarPtr *string
	if req.Name != "" {
		namePtr = &req.Name
	}
	if req.AvatarURL != "" {
		avatarPtr = &req.AvatarURL
	}

	if err := s.repo.UpdateUserProfile(userID, namePtr, avatarPtr); err != nil {
		return ProfileResponse{}, NewInternalError("Unable to update profile", err)
	}

	user, err := s.repo.FindActiveUserByID(userID)
	if err != nil {
		return ProfileResponse{}, NewInternalError("Failed to load updated profile", err)
	}
	return s.buildProfileResponse(user)
}

// UpdatePreferences returns user preferences.
func (s *Service) UpdatePreferences(userID int64, req UpdatePreferencesRequest) (PreferencesResponse, error) {
	if _, err := s.repo.FindActiveUserByID(userID); err != nil {
		return PreferencesResponse{}, NewNotFoundError("User not found", err)
	}
	return PreferencesResponse(req), nil
}

// FindUserByID retrieves a user record by ID.
func (s *Service) FindUserByID(userID int64) (UserRecord, error) {
	return s.repo.FindUserByID(userID)
}

// FindActiveUserByID retrieves an active user record by ID.
func (s *Service) FindActiveUserByID(userID int64) (UserRecord, error) {
	return s.repo.FindActiveUserByID(userID)
}

// UpdateUserTeams updates the teams associated with a user.
func (s *Service) UpdateUserTeams(userID int64, teamsJSON string) error {
	return s.repo.UpdateUserTeams(userID, teamsJSON)
}

func (s *Service) buildUserResponse(user UserRecord) (UserResponse, error) {
	memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
	teamIDs := TeamIDsFromMemberships(memberships)
	roleByTeamID := make(map[int64]string, len(memberships))
	for _, membership := range memberships {
		roleByTeamID[membership.TeamID] = membership.Role
	}

	teams := make([]UserTeamDetail, 0)
	if len(teamIDs) > 0 {
		records, err := s.repo.ListActiveTeamsByIDs(teamIDs)
		if err != nil {
			return UserResponse{}, NewInternalError("Failed to load teams", err)
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

func (s *Service) buildProfileResponse(user UserRecord) (ProfileResponse, error) {
	memberships, _ := ParseTeamMemberships(ValueOr(user.TeamsJSON, "[]"))
	teamIDs := TeamIDsFromMemberships(memberships)

	teams := make([]ProfileTeam, 0)
	if len(teamIDs) > 0 {
		records, err := s.repo.ListActiveTeamsByIDs(teamIDs)
		if err != nil {
			return ProfileResponse{}, NewInternalError("Failed to load teams", err)
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
