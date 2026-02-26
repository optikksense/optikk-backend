package model

// SavedQuery represents a query definition stored for team-wide reuse.
type SavedQuery struct {
	ID              int64          `json:"id"`
	OrganizationID  int64          `json:"organizationId"`
	TeamID          int64          `json:"teamId"`
	QueryType       string         `json:"queryType"`
	Name            string         `json:"name"`
	Description     string         `json:"description"`
	Query           map[string]any `json:"query"`
	CreatedByUserID int64          `json:"createdByUserId"`
	CreatedByEmail  string         `json:"createdByEmail"`
	CreatedAt       string         `json:"createdAt"`
	UpdatedAt       string         `json:"updatedAt"`
}

// SavedQueryInput is used by create/update operations.
type SavedQueryInput struct {
	OrganizationID  int64
	TeamID          int64
	QueryType       string
	Name            string
	Description     string
	Query           any
	CreatedByUserID int64
	CreatedByEmail  string
}
