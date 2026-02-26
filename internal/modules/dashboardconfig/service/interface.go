package service

// Service encapsulates the business logic for dashboard configurations.
type Service interface {
	EnsureTable() error
	GetConfig(teamID int64, pageID string) (string, error)
	SaveConfig(teamID int64, pageID, configYaml string) error
}
