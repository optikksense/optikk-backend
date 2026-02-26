package model

type HealthCheck struct {
	ID              int64   `json:"id"`
	TeamID          int64   `json:"team_id"`
	OrganizationID  int64   `json:"organization_id"`
	Name            string  `json:"name"`
	Type            string  `json:"type"`
	TargetURL       string  `json:"target_url"`
	IntervalSeconds int     `json:"interval_seconds"`
	TimeoutMs       int     `json:"timeout_ms"`
	ExpectedStatus  int     `json:"expected_status"`
	Enabled         bool    `json:"enabled"`
	Tags            *string `json:"tags"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

type HealthCheckStatus struct {
	CheckID       string  `json:"check_id"`
	CheckName     string  `json:"check_name"`
	CheckType     string  `json:"check_type"`
	TargetURL     string  `json:"target_url"`
	UpCount       int64   `json:"up_count"`
	DownCount     int64   `json:"down_count"`
	DegradedCount int64   `json:"degraded_count"`
	TotalChecks   int64   `json:"total_checks"`
	UptimePct     float64 `json:"uptime_pct"`
	AvgResponseMs float64 `json:"avg_response_ms"`
	LastChecked   string  `json:"last_checked"`
	CurrentStatus string  `json:"current_status"`
}

type HealthCheckResult struct {
	Timestamp      string  `json:"timestamp"`
	Status         string  `json:"status"`
	ResponseTimeMs float64 `json:"response_time_ms"`
	HTTPStatusCode int     `json:"http_status_code"`
	ErrorMessage   *string `json:"error_message"`
	Region         string  `json:"region"`
}

type HealthCheckTrend struct {
	TimeBucket    string  `json:"time_bucket"`
	AvgResponseMs float64 `json:"avg_response_ms"`
	MinResponseMs float64 `json:"min_response_ms"`
	MaxResponseMs float64 `json:"max_response_ms"`
	Status        string  `json:"status"`
	UpCount       int64   `json:"up_count"`
	DownCount     int64   `json:"down_count"`
}
