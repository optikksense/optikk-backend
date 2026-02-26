package store

import (
	"encoding/json"
	"time"

	"github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/deployments/model"
)

// ClickHouseRepository encapsulates data access logic for deployment tracking.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Deployments Repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetDeployments returns a paginated list of deployments with optional filters.
func (r *ClickHouseRepository) GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]model.Deployment, int64, error) {
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

	deployments := make([]model.Deployment, len(rows))
	for i, row := range rows {
		deployments[i] = rowToDeployment(row)
	}

	return deployments, total, nil
}

// GetDeploymentEvents returns a lightweight event list for timeline overlays.
func (r *ClickHouseRepository) GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]model.DeploymentEvent, error) {
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

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	events := make([]model.DeploymentEvent, len(rows))
	for i, row := range rows {
		events[i] = rowToDeploymentEvent(row)
	}
	return events, nil
}

// GetDeploymentDiff returns before/after performance comparison around a deploy.
func (r *ClickHouseRepository) GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (*model.DeploymentDiff, error) {
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
	deployTime := dbutil.TimeFromAny(deploy["deploy_time"])
	if deployTime.IsZero() {
		deployTime = time.Now().UTC()
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

	return &model.DeploymentDiff{
		DeployID:           deployID,
		DeployTime:         deployTime.UTC(),
		ServiceName:        serviceName,
		WindowMinutes:      windowMinutes,
		AvgLatencyBefore:   dbutil.Float64FromAny(before["avg_latency"]),
		P95LatencyBefore:   dbutil.Float64FromAny(before["p95_latency"]),
		ErrorRateBefore:    dbutil.Float64FromAny(before["error_rate"]),
		RequestCountBefore: dbutil.Int64FromAny(before["request_count"]),
		AvgLatencyAfter:    dbutil.Float64FromAny(after["avg_latency"]),
		P95LatencyAfter:    dbutil.Float64FromAny(after["p95_latency"]),
		ErrorRateAfter:     dbutil.Float64FromAny(after["error_rate"]),
		RequestCountAfter:  dbutil.Int64FromAny(after["request_count"]),
	}, nil
}

// CreateDeployment records a new deployment event.
func (r *ClickHouseRepository) CreateDeployment(teamUUID string, deployID string, req contracts.DeploymentCreateRequest) error {
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

func rowToDeployment(row map[string]any) model.Deployment {
	var attrs map[string]any
	if attrStr := dbutil.StringFromAny(row["attributes"]); attrStr != "" {
		_ = json.Unmarshal([]byte(attrStr), &attrs)
	}

	return model.Deployment{
		DeployID:        dbutil.StringFromAny(row["deploy_id"]),
		ServiceName:     dbutil.StringFromAny(row["service_name"]),
		Version:         dbutil.StringFromAny(row["version"]),
		Environment:     dbutil.StringFromAny(row["environment"]),
		DeployedBy:      dbutil.StringFromAny(row["deployed_by"]),
		DeployTime:      dbutil.TimeFromAny(row["deploy_time"]),
		Status:          dbutil.StringFromAny(row["status"]),
		CommitSHA:       dbutil.StringFromAny(row["commit_sha"]),
		DurationSeconds: int(dbutil.Int64FromAny(row["duration_seconds"])),
		Attributes:      attrs,
	}
}

func rowToDeploymentEvent(row map[string]any) model.DeploymentEvent {
	return model.DeploymentEvent{
		DeployID:    dbutil.StringFromAny(row["deploy_id"]),
		ServiceName: dbutil.StringFromAny(row["service_name"]),
		Version:     dbutil.StringFromAny(row["version"]),
		DeployTime:  dbutil.TimeFromAny(row["deploy_time"]),
		Status:      dbutil.StringFromAny(row["status"]),
		Environment: dbutil.StringFromAny(row["environment"]),
	}
}
