package inventory

import "time"

type ServiceObservation struct {
	TeamID        int64
	ServiceName   string
	DisplayName   string
	OwnerTeam     string
	OwnerName     string
	OnCall        string
	Tier          string
	Environment   string
	Runtime       string
	Language      string
	RepositoryURL string
	RunbookURL    string
	DashboardURL  string
	ServiceType   string
	ClusterName   string
	Tags          []string
	FirstSeenAt   time.Time
	LastSeenAt    time.Time
}

type DependencyObservation struct {
	TeamID         int64
	SourceService  string
	TargetService  string
	DependencyKind string
	FirstSeenAt    time.Time
	LastSeenAt     time.Time
}

type ServiceRecord struct {
	TeamID        int64
	ServiceName   string
	DisplayName   string
	OwnerTeam     string
	OwnerName     string
	OnCall        string
	Tier          string
	Environment   string
	Runtime       string
	Language      string
	RepositoryURL string
	RunbookURL    string
	DashboardURL  string
	ServiceType   string
	ClusterName   string
	Tags          []string
	FirstSeenAt   string
	LastSeenAt    string
}

type DependencyRecord struct {
	TeamID         int64
	SourceService  string
	TargetService  string
	DependencyKind string
	FirstSeenAt    string
	LastSeenAt     string
}
