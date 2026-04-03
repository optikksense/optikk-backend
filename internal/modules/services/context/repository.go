package servicecontext

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

type catalogSeed struct {
	ServiceName   string
	DisplayName   string
	Description   string
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
}

type catalogRecord = ServiceCatalog

type metricsRollup struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatency   float64 `ch:"avg_latency"`
	P95Latency   float64 `ch:"p95_latency"`
}

type Repository interface {
	UpsertCatalogDefaults(teamID int64, seeds []catalogSeed) error
	FindCatalog(teamID int64, serviceName string) (catalogRecord, error)
	ListCatalog(teamID int64) ([]catalogRecord, error)
	ListDeployments(teamID int64, serviceName string, limit int) ([]ServiceDeployment, error)
	ListTeamDeployments(teamID int64, limit int) ([]ServiceDeployment, error)
	ListIncidents(teamID int64, serviceName string, limit int) ([]ServiceIncident, error)
	ListTeamIncidents(teamID int64, limit int) ([]ServiceIncident, error)
	ListChangeEvents(teamID int64, serviceName string, limit int) ([]ServiceChangeEvent, error)
}

type MetricsRepository interface {
	ListServiceMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]metricsRollup, error)
}

type MySQLRepository struct {
	db dbutil.Querier
}

type ClickHouseMetricsRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: dbutil.NewMySQLWrapper(db, appConfig.CircuitBreakerConsecutiveFailures(), appConfig.CircuitBreakerResetTimeout()),
	}
}

func NewMetricsRepository(db *dbutil.NativeQuerier) *ClickHouseMetricsRepository {
	return &ClickHouseMetricsRepository{db: db}
}

func (r *MySQLRepository) UpsertCatalogDefaults(teamID int64, seeds []catalogSeed) error {
	for _, seed := range seeds {
		if strings.TrimSpace(seed.ServiceName) == "" {
			continue
		}
		_, err := r.db.Exec(`
			INSERT INTO service_catalog_entries (
				team_id, service_name, display_name, description, owner_team, owner_name, on_call,
				tier, environment, runtime, language, repository_url, runbook_url, dashboard_url,
				service_type, cluster_name, tags
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP
		`,
			teamID,
			seed.ServiceName,
			seed.DisplayName,
			dbutil.NullableString(seed.Description),
			seed.OwnerTeam,
			seed.OwnerName,
			seed.OnCall,
			seed.Tier,
			seed.Environment,
			seed.Runtime,
			seed.Language,
			dbutil.NullableString(seed.RepositoryURL),
			dbutil.NullableString(seed.RunbookURL),
			dbutil.NullableString(seed.DashboardURL),
			seed.ServiceType,
			seed.ClusterName,
			dbutil.JSONString(seed.Tags),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *MySQLRepository) FindCatalog(teamID int64, serviceName string) (catalogRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT service_name, display_name, description, owner_team, owner_name, on_call, tier,
		       environment, runtime, language, repository_url, runbook_url, dashboard_url,
		       service_type, cluster_name, tags
		FROM service_catalog_entries
		WHERE team_id = ? AND service_name = ?
		LIMIT 1
	`, teamID, serviceName)
	if err != nil {
		return catalogRecord{}, err
	}
	if len(row) == 0 {
		return catalogRecord{}, sql.ErrNoRows
	}
	return catalogRecordFromMap(row), nil
}

func (r *MySQLRepository) ListCatalog(teamID int64) ([]catalogRecord, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, display_name, description, owner_team, owner_name, on_call, tier,
		       environment, runtime, language, repository_url, runbook_url, dashboard_url,
		       service_type, cluster_name, tags
		FROM service_catalog_entries
		WHERE team_id = ?
		ORDER BY service_name ASC
	`, teamID)
	if err != nil {
		return nil, err
	}
	result := make([]catalogRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, catalogRecordFromMap(row))
	}
	return result, nil
}

func (r *MySQLRepository) ListDeployments(teamID int64, serviceName string, limit int) ([]ServiceDeployment, error) {
	rows, err := dbutil.QueryMapsLimit(r.db, limit, `
		SELECT id, service_name, version, environment, status, summary, deployed_by, commit_sha,
		       started_at, finished_at, change_summary
		FROM service_deployments
		WHERE team_id = ? AND service_name = ?
		ORDER BY started_at DESC
	`, teamID, serviceName)
	if err != nil {
		return nil, err
	}
	return deploymentsFromMaps(rows), nil
}

func (r *MySQLRepository) ListTeamDeployments(teamID int64, limit int) ([]ServiceDeployment, error) {
	rows, err := dbutil.QueryMapsLimit(r.db, limit, `
		SELECT id, service_name, version, environment, status, summary, deployed_by, commit_sha,
		       started_at, finished_at, change_summary
		FROM service_deployments
		WHERE team_id = ?
		ORDER BY started_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return deploymentsFromMaps(rows), nil
}

func (r *MySQLRepository) ListIncidents(teamID int64, serviceName string, limit int) ([]ServiceIncident, error) {
	rows, err := dbutil.QueryMapsLimit(r.db, limit, `
		SELECT id, service_name, title, severity, status, summary, commander, started_at, resolved_at
		FROM service_incidents
		WHERE team_id = ? AND service_name = ?
		ORDER BY started_at DESC
	`, teamID, serviceName)
	if err != nil {
		return nil, err
	}
	return incidentsFromMaps(rows), nil
}

func (r *MySQLRepository) ListTeamIncidents(teamID int64, limit int) ([]ServiceIncident, error) {
	rows, err := dbutil.QueryMapsLimit(r.db, limit, `
		SELECT id, service_name, title, severity, status, summary, commander, started_at, resolved_at
		FROM service_incidents
		WHERE team_id = ?
		ORDER BY started_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	return incidentsFromMaps(rows), nil
}

func (r *MySQLRepository) ListChangeEvents(teamID int64, serviceName string, limit int) ([]ServiceChangeEvent, error) {
	rows, err := dbutil.QueryMapsLimit(r.db, limit, `
		SELECT id, service_name, event_type, title, summary, related_reference, happened_at
		FROM service_change_events
		WHERE team_id = ? AND service_name = ?
		ORDER BY happened_at DESC
	`, teamID, serviceName)
	if err != nil {
		return nil, err
	}
	return changeEventsFromMaps(rows), nil
}

func (r *ClickHouseMetricsRepository) ListServiceMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]metricsRollup, error) {
	var rows []metricsRollup
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       toInt64(count()) AS request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       avg(s.duration_nano / 1000000.0) AS avg_latency,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND (s.parent_span_id = '' OR s.parent_span_id = '0000000000000000')
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name
		ORDER BY request_count DESC
	`, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func catalogRecordFromMap(row map[string]any) catalogRecord {
	return catalogRecord{
		ServiceName:   dbutil.StringFromAny(row["service_name"]),
		DisplayName:   dbutil.StringFromAny(row["display_name"]),
		Description:   dbutil.StringFromAny(row["description"]),
		OwnerTeam:     dbutil.StringFromAny(row["owner_team"]),
		OwnerName:     dbutil.StringFromAny(row["owner_name"]),
		OnCall:        dbutil.StringFromAny(row["on_call"]),
		Tier:          dbutil.StringFromAny(row["tier"]),
		Environment:   dbutil.StringFromAny(row["environment"]),
		Runtime:       dbutil.StringFromAny(row["runtime"]),
		Language:      dbutil.StringFromAny(row["language"]),
		RepositoryURL: dbutil.StringFromAny(row["repository_url"]),
		RunbookURL:    dbutil.StringFromAny(row["runbook_url"]),
		DashboardURL:  dbutil.StringFromAny(row["dashboard_url"]),
		ServiceType:   dbutil.StringFromAny(row["service_type"]),
		ClusterName:   dbutil.StringFromAny(row["cluster_name"]),
		Tags:          parseStringList(dbutil.StringFromAny(row["tags"])),
	}
}

func deploymentsFromMaps(rows []map[string]any) []ServiceDeployment {
	result := make([]ServiceDeployment, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceDeployment{
			ID:            dbutil.Int64FromAny(row["id"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			Version:       dbutil.StringFromAny(row["version"]),
			Environment:   dbutil.StringFromAny(row["environment"]),
			Status:        dbutil.StringFromAny(row["status"]),
			Summary:       dbutil.StringFromAny(row["summary"]),
			DeployedBy:    dbutil.StringFromAny(row["deployed_by"]),
			CommitSHA:     dbutil.StringFromAny(row["commit_sha"]),
			StartedAt:     dbutil.StringFromAny(row["started_at"]),
			FinishedAt:    dbutil.StringFromAny(row["finished_at"]),
			ChangeSummary: dbutil.StringFromAny(row["change_summary"]),
		})
	}
	return result
}

func incidentsFromMaps(rows []map[string]any) []ServiceIncident {
	result := make([]ServiceIncident, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceIncident{
			ID:          dbutil.Int64FromAny(row["id"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Title:       dbutil.StringFromAny(row["title"]),
			Severity:    dbutil.StringFromAny(row["severity"]),
			Status:      dbutil.StringFromAny(row["status"]),
			Summary:     dbutil.StringFromAny(row["summary"]),
			Commander:   dbutil.StringFromAny(row["commander"]),
			StartedAt:   dbutil.StringFromAny(row["started_at"]),
			ResolvedAt:  dbutil.StringFromAny(row["resolved_at"]),
		})
	}
	return result
}

func changeEventsFromMaps(rows []map[string]any) []ServiceChangeEvent {
	result := make([]ServiceChangeEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceChangeEvent{
			ID:               dbutil.Int64FromAny(row["id"]),
			ServiceName:      dbutil.StringFromAny(row["service_name"]),
			EventType:        dbutil.StringFromAny(row["event_type"]),
			Title:            dbutil.StringFromAny(row["title"]),
			Summary:          dbutil.StringFromAny(row["summary"]),
			RelatedReference: dbutil.StringFromAny(row["related_reference"]),
			HappenedAt:       dbutil.StringFromAny(row["happened_at"]),
		})
	}
	return result
}

func parseStringList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return []string{}
	}
	return values
}
