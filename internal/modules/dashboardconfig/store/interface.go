package store

// Repository encapsulates data access for dashboard chart configurations.
type Repository interface {
	EnsureTable() error
	GetConfig(teamID int64, pageID string) (string, error)
	SaveConfig(teamID int64, pageID, configYaml string) error
}
