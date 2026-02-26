package impl

import (
	"database/sql"
	"time"

	"github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/health/model"
)

// ClickHouseRepository encapsulates data access logic for health checks.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Health Repository.
func NewRepository(db *sql.DB) *ClickHouseRepository {
	return &ClickHouseRepository{db: dbutil.NewMySQLWrapper(db)}
}

// GetHealthChecks returns a list of all health checks for a tenant team.
func (r *ClickHouseRepository) GetHealthChecks(teamID int64) ([]model.HealthCheck, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, team_id, organization_id, name, type, target_url, interval_seconds,
		       timeout_ms, expected_status, enabled, tags, created_at, updated_at
		FROM health_checks
		WHERE team_id = ?
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}

	checks := make([]model.HealthCheck, len(rows))
	for i, row := range rows {
		checks[i] = rowToHealthCheck(row)
	}
	return checks, nil
}

// GetHealthCheck returns a single health check by ID.
func (r *ClickHouseRepository) GetHealthCheck(id int64) (model.HealthCheck, error) {
	row, err := dbutil.QueryMap(r.db, `SELECT * FROM health_checks WHERE id = ?`, id)
	if err != nil {
		return model.HealthCheck{}, err
	}
	if len(row) == 0 {
		return model.HealthCheck{}, sql.ErrNoRows
	}
	return rowToHealthCheck(row), nil
}

// CreateHealthCheck creates a new health check and returns it.
func (r *ClickHouseRepository) CreateHealthCheck(teamID, orgID int64, req contracts.HealthCheckRequest) (model.HealthCheck, error) {
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
		return model.HealthCheck{}, err
	}
	id, _ := res.LastInsertId()
	return r.GetHealthCheck(id)
}

// UpdateHealthCheck updates an existing health check and returns it.
func (r *ClickHouseRepository) UpdateHealthCheck(id int64, req contracts.HealthCheckRequest) (model.HealthCheck, error) {
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
		return model.HealthCheck{}, err
	}
	return r.GetHealthCheck(id)
}

// DeleteHealthCheck removes a health check.
func (r *ClickHouseRepository) DeleteHealthCheck(id int64) error {
	_, err := r.db.Exec(`DELETE FROM health_checks WHERE id = ?`, id)
	return err
}

// ToggleHealthCheck flips the enabled status of a health check and returns it.
func (r *ClickHouseRepository) ToggleHealthCheck(id int64) (model.HealthCheck, error) {
	_, err := r.db.Exec(`UPDATE health_checks SET enabled = NOT enabled, updated_at = ? WHERE id = ?`, time.Now().UTC(), id)
	if err != nil {
		return model.HealthCheck{}, err
	}
	return r.GetHealthCheck(id)
}

// GetHealthCheckStatus returns aggregated uptime/response time summary per check.
func (r *ClickHouseRepository) GetHealthCheckStatus(teamUUID string, startMs, endMs int64) ([]model.HealthCheckStatus, error) {
	rows, err := dbutil.QueryMaps(r.db, `
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
	if err != nil {
		return nil, err
	}

	statuses := make([]model.HealthCheckStatus, len(rows))
	for i, row := range rows {
		statuses[i] = model.HealthCheckStatus{
			CheckID:       dbutil.StringFromAny(row["check_id"]),
			CheckName:     dbutil.StringFromAny(row["check_name"]),
			CheckType:     dbutil.StringFromAny(row["check_type"]),
			TargetURL:     dbutil.StringFromAny(row["target_url"]),
			UpCount:       dbutil.Int64FromAny(row["up_count"]),
			DownCount:     dbutil.Int64FromAny(row["down_count"]),
			DegradedCount: dbutil.Int64FromAny(row["degraded_count"]),
			TotalChecks:   dbutil.Int64FromAny(row["total_checks"]),
			UptimePct:     dbutil.Float64FromAny(row["uptime_pct"]),
			AvgResponseMs: dbutil.Float64FromAny(row["avg_response_ms"]),
			LastChecked:   dbutil.StringFromAny(row["last_checked"]),
			CurrentStatus: dbutil.StringFromAny(row["current_status"]),
		}
	}
	return statuses, nil
}

// GetHealthCheckResults returns individual check results for a specific check.
func (r *ClickHouseRepository) GetHealthCheckResults(teamUUID string, checkID string, startMs, endMs int64, limit, offset int) ([]model.HealthCheckResult, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, status, response_time_ms, http_status_code, error_message, region
		FROM health_check_results
		WHERE team_id = ? AND check_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, teamUUID, checkID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit, offset)
	if err != nil {
		return nil, err
	}

	results := make([]model.HealthCheckResult, len(rows))
	for i, row := range rows {
		results[i] = model.HealthCheckResult{
			Timestamp:      dbutil.StringFromAny(row["timestamp"]),
			Status:         dbutil.StringFromAny(row["status"]),
			ResponseTimeMs: dbutil.Float64FromAny(row["response_time_ms"]),
			HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
			ErrorMessage:   dbutil.NullableStringFromAny(row["error_message"]),
			Region:         dbutil.StringFromAny(row["region"]),
		}
	}
	return results, nil
}

// GetHealthCheckTrend returns aggregated response-time trend for a check.
func (r *ClickHouseRepository) GetHealthCheckTrend(teamUUID string, checkID string, startMs, endMs int64) ([]model.HealthCheckTrend, error) {
	rows, err := dbutil.QueryMaps(r.db, `
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
	if err != nil {
		return nil, err
	}

	trends := make([]model.HealthCheckTrend, len(rows))
	for i, row := range rows {
		trends[i] = model.HealthCheckTrend{
			TimeBucket:    dbutil.StringFromAny(row["time_bucket"]),
			AvgResponseMs: dbutil.Float64FromAny(row["avg_response_ms"]),
			MinResponseMs: dbutil.Float64FromAny(row["min_response_ms"]),
			MaxResponseMs: dbutil.Float64FromAny(row["max_response_ms"]),
			Status:        dbutil.StringFromAny(row["status"]),
			UpCount:       dbutil.Int64FromAny(row["up_count"]),
			DownCount:     dbutil.Int64FromAny(row["down_count"]),
		}
	}
	return trends, nil
}

func rowToHealthCheck(row map[string]any) model.HealthCheck {
	return model.HealthCheck{
		ID:              dbutil.Int64FromAny(row["id"]),
		TeamID:          dbutil.Int64FromAny(row["team_id"]),
		OrganizationID:  dbutil.Int64FromAny(row["organization_id"]),
		Name:            dbutil.StringFromAny(row["name"]),
		Type:            dbutil.StringFromAny(row["type"]),
		TargetURL:       dbutil.StringFromAny(row["target_url"]),
		IntervalSeconds: int(dbutil.Int64FromAny(row["interval_seconds"])),
		TimeoutMs:       int(dbutil.Int64FromAny(row["timeout_ms"])),
		ExpectedStatus:  int(dbutil.Int64FromAny(row["expected_status"])),
		Enabled:         dbutil.Int64FromAny(row["enabled"]) != 0,
		Tags:            dbutil.NullableStringFromAny(row["tags"]),
		CreatedAt:       dbutil.StringFromAny(row["created_at"]),
		UpdatedAt:       dbutil.StringFromAny(row["updated_at"]),
	}
}
