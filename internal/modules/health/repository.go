package health

import (
	"database/sql"
	"time"

	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for health checks.
type Repository struct {
	db dbutil.Querier
}

// NewRepository creates a new Health Repository.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: dbutil.NewMySQLWrapper(db)}
}

// GetHealthChecks returns a list of all health checks for a tenant team.
func (r *Repository) GetHealthChecks(teamID int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT id, team_id, organization_id, name, type, target_url, interval_seconds,
		       timeout_ms, expected_status, enabled, tags, created_at, updated_at
		FROM health_checks
		WHERE team_id = ?
		ORDER BY created_at DESC
	`, teamID)
}

// GetHealthCheck returns a single health check by ID.
func (r *Repository) GetHealthCheck(id int64) (map[string]any, error) {
	return dbutil.QueryMap(r.db, `SELECT * FROM health_checks WHERE id = ?`, id)
}

// CreateHealthCheck creates a new health check and returns it.
func (r *Repository) CreateHealthCheck(teamID, orgID int64, req types.HealthCheckRequest) (map[string]any, error) {
	interval := 60
	timeout := 5000
	expected := 200
	enabled := true
	if req.IntervalSeconds != nil {
		interval = *req.IntervalSeconds
	}
	if req.TimeoutMs != nil {
		timeout = *req.TimeoutMs
	}
	if req.ExpectedStatus != nil {
		expected = *req.ExpectedStatus
	}
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	res, err := r.db.Exec(`
		INSERT INTO health_checks (
			team_id, organization_id, name, type, target_url,
			interval_seconds, timeout_ms, expected_status, enabled, tags, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, teamID, orgID, req.Name, req.Type, req.TargetURL,
		interval, timeout, expected, enabled, dbutil.NullableString(req.Tags), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return r.GetHealthCheck(id)
}

// UpdateHealthCheck updates an existing health check and returns it.
func (r *Repository) UpdateHealthCheck(id int64, req types.HealthCheckRequest) (map[string]any, error) {
	_, err := r.db.Exec(`
		UPDATE health_checks
		SET name = ?, type = ?, target_url = ?,
		    interval_seconds = COALESCE(?, interval_seconds),
		    timeout_ms = COALESCE(?, timeout_ms),
		    expected_status = COALESCE(?, expected_status),
		    enabled = COALESCE(?, enabled),
		    tags = COALESCE(?, tags),
		    updated_at = ?
		WHERE id = ?
	`, req.Name, req.Type, req.TargetURL,
		req.IntervalSeconds, req.TimeoutMs, req.ExpectedStatus, req.Enabled,
		dbutil.NullableString(req.Tags), time.Now().UTC(), id)
	if err != nil {
		return nil, err
	}
	return r.GetHealthCheck(id)
}

// DeleteHealthCheck removes a health check.
func (r *Repository) DeleteHealthCheck(id int64) error {
	_, err := r.db.Exec(`DELETE FROM health_checks WHERE id = ?`, id)
	return err
}

// ToggleHealthCheck flips the enabled status of a health check and returns it.
func (r *Repository) ToggleHealthCheck(id int64) (map[string]any, error) {
	_, err := r.db.Exec(`UPDATE health_checks SET enabled = NOT enabled, updated_at = ? WHERE id = ?`, time.Now().UTC(), id)
	if err != nil {
		return nil, err
	}
	return r.GetHealthCheck(id)
}

// GetHealthCheckStatus returns aggregated uptime/response time summary per check.
func (r *Repository) GetHealthCheckStatus(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT check_id, check_name, check_type, target_url,
		       SUM(CASE WHEN status='up' THEN 1 ELSE 0 END) as up_count,
		       SUM(CASE WHEN status='down' THEN 1 ELSE 0 END) as down_count,
		       SUM(CASE WHEN status='degraded' THEN 1 ELSE 0 END) as degraded_count,
		       COUNT(*) as total_checks,
		       IF(COUNT(*) > 0, SUM(CASE WHEN status='up' THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as uptime_pct,
		       AVG(response_time_ms) as avg_response_ms,
		       MAX(timestamp) as last_checked,
		       SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY timestamp DESC), ',', 1) as current_status
		FROM health_check_results
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY check_id, check_name, check_type, target_url
		ORDER BY check_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetHealthCheckResults returns individual check results for a specific check.
func (r *Repository) GetHealthCheckResults(teamUUID string, checkID string, startMs, endMs int64, limit, offset int) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT timestamp, status, response_time_ms, http_status_code, error_message, region
		FROM health_check_results
		WHERE team_id = ? AND check_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, teamUUID, checkID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit, offset)
}

// GetHealthCheckTrend returns aggregated response-time trend for a check.
func (r *Repository) GetHealthCheckTrend(teamUUID string, checkID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') as time_bucket,
		       AVG(response_time_ms) as avg_response_ms,
		       MIN(response_time_ms) as min_response_ms,
		       MAX(response_time_ms) as max_response_ms,
		       SUBSTRING_INDEX(GROUP_CONCAT(status ORDER BY timestamp DESC), ',', 1) as status,
		       SUM(CASE WHEN status='up' THEN 1 ELSE 0 END) as up_count,
		       SUM(CASE WHEN status='down' THEN 1 ELSE 0 END) as down_count
		FROM health_check_results
		WHERE team_id = ? AND check_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')
		ORDER BY time_bucket ASC
	`, teamUUID, checkID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}
