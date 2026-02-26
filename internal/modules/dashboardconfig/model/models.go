package model

// DashboardConfig represents a dashboard page chart configuration.
type DashboardConfig struct {
	TeamID     int64  `json:"team_id"`
	PageID     string `json:"page_id"`
	ConfigYaml string `json:"config_yaml"`
}
