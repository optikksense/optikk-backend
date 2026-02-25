package deployments

import (
	"time"

	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for deployment tracking.
type Repository struct {
	db dbutil.Querier
}

// NewRepository creates a new Deployments Repository.
func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// GetDeployments returns a paginated list of deployments with optional filters.
func (r *Repository) GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]map[string]any, int64, error) {
	query := `
		SELECT deploy_id, service_name, version, environment, deployed_by, deploy_time,
		       status, commit_sha, duration_seconds, attributes
		FROM deployments
		WHERE team_id = ? AND deploy_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	if environment != "" {
		query += ` AND environment = ?`
		args = append(args, environment)
	}
	query += ` ORDER BY deploy_time DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, 0, err
	}

	total := dbutil.QueryCount(r.db, `
		SELECT COUNT(*) FROM deployments WHERE team_id = ? AND deploy_time BETWEEN ? AND ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	return rows, total, nil
}

// GetDeploymentEvents returns a lightweight event list for timeline overlays.
func (r *Repository) GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT deploy_id, service_name, version, deploy_time, status, environment
		FROM deployments
		WHERE team_id = ? AND deploy_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` ORDER BY deploy_time ASC`
	return dbutil.QueryMaps(r.db, query, args...)
}

// GetDeploymentDiff returns before/after performance comparison around a deploy.
func (r *Repository) GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (map[string]any, error) {
	deploy, err := dbutil.QueryMap(r.db, `
		SELECT deploy_time, service_name
		FROM deployments
		WHERE team_id = ? AND deploy_id = ?
		LIMIT 1
	`, teamUUID, deployID)

	if err != nil || len(deploy) == 0 {
		return nil, nil // No deploy found
	}

	serviceName := dbutil.StringFromAny(deploy["service_name"])
	deployTime, _ := time.Parse(time.RFC3339, dbutil.StringFromAny(deploy["deploy_time"]))
	if deployTime.IsZero() {
		if t, ok := deploy["deploy_time"].(time.Time); ok {
			deployTime = t
		} else {
			deployTime = time.Now().UTC()
		}
	}

	beforeFrom := deployTime.Add(-time.Duration(windowMinutes) * time.Minute)
	beforeTo := deployTime
	afterFrom := deployTime
	afterTo := deployTime.Add(time.Duration(windowMinutes) * time.Minute)

	before, _ := dbutil.QueryMap(r.db, `
		SELECT AVG(duration_ms) as avg_latency, AVG(duration_ms) as p95_latency,
		       IF(COUNT(*)>0, SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as error_rate,
		       COUNT(*) as request_count
		FROM spans
		WHERE team_id = ? AND service_name = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
	`, teamUUID, serviceName, beforeFrom, beforeTo)

	after, _ := dbutil.QueryMap(r.db, `
		SELECT AVG(duration_ms) as avg_latency, AVG(duration_ms) as p95_latency,
		       IF(COUNT(*)>0, SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as error_rate,
		       COUNT(*) as request_count
		FROM spans
		WHERE team_id = ? AND service_name = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
	`, teamUUID, serviceName, afterFrom, afterTo)

	return map[string]any{
		"deploy_id":            deployID,
		"deploy_time":          deployTime.UTC(),
		"service_name":         serviceName,
		"window_minutes":       windowMinutes,
		"avg_latency_before":   before["avg_latency"],
		"p95_latency_before":   before["p95_latency"],
		"error_rate_before":    before["error_rate"],
		"request_count_before": before["request_count"],
		"avg_latency_after":    after["avg_latency"],
		"p95_latency_after":    after["p95_latency"],
		"error_rate_after":     after["error_rate"],
		"request_count_after":  after["request_count"],
	}, nil
}

// CreateDeployment records a new deployment event.
func (r *Repository) CreateDeployment(teamUUID string, deployID string, req types.DeploymentCreateRequest) error {
	dur := 0
	if req.DurationSeconds != nil {
		dur = *req.DurationSeconds
	}
	attrs := dbutil.JSONString(req.Attributes)
	_, err := r.db.Exec(`
		INSERT INTO deployments (
			team_id, deploy_id, service_name, version, environment, deployed_by,
			deploy_time, status, commit_sha, duration_seconds, attributes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, teamUUID, deployID, req.ServiceName, req.Version,
		dbutil.DefaultString(req.Environment, "production"),
		dbutil.DefaultString(req.DeployedBy, ""),
		time.Now().UTC(),
		dbutil.DefaultString(req.Status, "success"),
		req.CommitSHA, dur, attrs)
	return err
}
