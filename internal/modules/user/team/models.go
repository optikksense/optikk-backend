package team

type TeamResponse struct {
	ID          int64  `json:"id"`
	OrgName     string `json:"org_name"`
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Description any    `json:"description"`
	Active      bool   `json:"active"`
	Color       string `json:"color"`
	Icon        *string `json:"icon"`
	APIKey      string `json:"api_key"`
	CreatedAt   any    `json:"created_at"`
}

type TeamSummary struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Slug    string `json:"slug"`
	Color   string `json:"color"`
	OrgName string `json:"orgName"`
	Role    string `json:"role"`
}
