package team

type CreateTeamRequest struct {
	TeamName    string `json:"team_name" binding:"required" validate:"required"`
	OrgName     string `json:"org_name" binding:"required" validate:"required"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Color       string `json:"color"`
}
