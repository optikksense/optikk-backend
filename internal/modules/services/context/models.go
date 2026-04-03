package servicecontext

type ServiceCatalog struct {
	ServiceName   string   `json:"service_name"`
	DisplayName   string   `json:"display_name"`
	Description   string   `json:"description"`
	OwnerTeam     string   `json:"owner_team"`
	OwnerName     string   `json:"owner_name"`
	OnCall        string   `json:"on_call"`
	Tier          string   `json:"tier"`
	Environment   string   `json:"environment"`
	Runtime       string   `json:"runtime"`
	Language      string   `json:"language"`
	RepositoryURL string   `json:"repository_url"`
	RunbookURL    string   `json:"runbook_url"`
	DashboardURL  string   `json:"dashboard_url"`
	ServiceType   string   `json:"service_type"`
	ClusterName   string   `json:"cluster_name"`
	Tags          []string `json:"tags"`
	LastSeenAt    string   `json:"last_seen_at"`
}

type ServiceOwnership struct {
	ServiceName string `json:"service_name"`
	OwnerTeam   string `json:"owner_team"`
	OwnerName   string `json:"owner_name"`
	OnCall      string `json:"on_call"`
}

type ServiceMonitor struct {
	Key       string  `json:"key"`
	Label     string  `json:"label"`
	Status    string  `json:"status"`
	Severity  string  `json:"severity"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Message   string  `json:"message"`
}

type ServiceMonitorSummary struct {
	ServiceName string           `json:"service_name"`
	Overall     string           `json:"overall"`
	Healthy     int              `json:"healthy"`
	Degraded    int              `json:"degraded"`
	Critical    int              `json:"critical"`
	Items       []ServiceMonitor `json:"items"`
}

type ServiceDeployment struct {
	ID            int64  `json:"id"`
	ServiceName   string `json:"service_name"`
	Version       string `json:"version"`
	Environment   string `json:"environment"`
	Status        string `json:"status"`
	Summary       string `json:"summary"`
	DeployedBy    string `json:"deployed_by"`
	CommitSHA     string `json:"commit_sha"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	ChangeSummary string `json:"change_summary"`
}

type ServiceIncident struct {
	ID          int64  `json:"id"`
	ServiceName string `json:"service_name"`
	Title       string `json:"title"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	Summary     string `json:"summary"`
	Commander   string `json:"commander"`
	StartedAt   string `json:"started_at"`
	ResolvedAt  string `json:"resolved_at"`
}

type ServiceChangeEvent struct {
	ID               int64  `json:"id"`
	ServiceName      string `json:"service_name"`
	EventType        string `json:"event_type"`
	Title            string `json:"title"`
	Summary          string `json:"summary"`
	RelatedReference string `json:"related_reference"`
	HappenedAt       string `json:"happened_at"`
}

type ServiceScorecardCheck struct {
	Key    string `json:"key"`
	Label  string `json:"label"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

type ServiceScorecard struct {
	ServiceName string                  `json:"service_name"`
	Score       int                     `json:"score"`
	Passed      int                     `json:"passed"`
	Total       int                     `json:"total"`
	Checks      []ServiceScorecardCheck `json:"checks"`
}

type ServiceCatalogRollup struct {
	ServiceCatalog
	HealthStatus           string  `json:"health_status"`
	RequestCount           int64   `json:"request_count"`
	ErrorCount             int64   `json:"error_count"`
	ErrorRate              float64 `json:"error_rate"`
	AvgLatency             float64 `json:"avg_latency"`
	P95Latency             float64 `json:"p95_latency"`
	MonitorStatus          string  `json:"monitor_status"`
	OpenIncidentCount      int     `json:"open_incident_count"`
	LatestIncidentSeverity string  `json:"latest_incident_severity"`
	LatestIncidentStatus   string  `json:"latest_incident_status"`
	LatestDeploymentStatus string  `json:"latest_deployment_status"`
	LatestDeploymentVer    string  `json:"latest_deployment_version"`
	ScorecardScore         int     `json:"scorecard_score"`
	HasTrafficInWindow     bool    `json:"has_traffic_in_window"`
}
