package contracts

type LoginRequest struct {
	Email    string `json:"email" binding:"required" example:"user@example.com"`
	Password string `json:"password" binding:"required" example:"securePassword123"`
}

type UserRequest struct {
	Email    string `json:"email"`
	Name     string `json:"name"`
	Role     string `json:"role"`
	Password string `json:"password"`
}

type SettingsRequest struct {
	Name        string         `json:"name"`
	AvatarURL   string         `json:"avatarUrl"`
	Preferences map[string]any `json:"preferences"`
}

type AlertRequest struct {
	Name            string   `json:"name" binding:"required"`
	Description     string   `json:"description"`
	Type            string   `json:"type" binding:"required"`
	Severity        string   `json:"severity" binding:"required"`
	ServiceName     string   `json:"serviceName"`
	Condition       string   `json:"condition"`
	Metric          string   `json:"metric"`
	Operator        string   `json:"operator"`
	Threshold       *float64 `json:"threshold"`
	DurationMinutes *int     `json:"durationMinutes"`
	RunbookURL      string   `json:"runbookUrl"`
}

type HealthCheckRequest struct {
	Name            string `json:"name" binding:"required"`
	Type            string `json:"type" binding:"required"`
	TargetURL       string `json:"targetUrl" binding:"required"`
	IntervalSeconds *int   `json:"intervalSeconds"`
	TimeoutMs       *int   `json:"timeoutMs"`
	ExpectedStatus  *int   `json:"expectedStatus"`
	Enabled         *bool  `json:"enabled"`
	Tags            string `json:"tags"`
}

type DeploymentCreateRequest struct {
	ServiceName     string         `json:"service_name"`
	Version         string         `json:"version"`
	Environment     string         `json:"environment"`
	DeployedBy      string         `json:"deployed_by"`
	Status          string         `json:"status"`
	CommitSHA       string         `json:"commit_sha"`
	DurationSeconds *int           `json:"duration_seconds"`
	Attributes      map[string]any `json:"attributes"`
}
