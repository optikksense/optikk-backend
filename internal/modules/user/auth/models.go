package auth

type AuthUserSummary struct {
	ID        int64  `json:"id"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	AvatarURL string `json:"avatarUrl"`
}

type AuthTeamSummary struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Color   string `json:"color"`
	OrgName string `json:"orgName"`
	Role    string `json:"role"`
}

type AuthContextResponse struct {
	User        AuthUserSummary   `json:"user"`
	Teams       []AuthTeamSummary `json:"teams"`
	CurrentTeam *AuthTeamSummary  `json:"currentTeam"`
}

type ValidateTokenResponse struct {
	Valid  bool   `json:"valid"`
	UserID int64  `json:"userId"`
	TeamID int64  `json:"teamId"`
	Role   string `json:"role"`
}

type MessageResponse struct {
	Message string `json:"message"`
}
