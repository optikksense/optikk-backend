package inventory

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

type Repository interface {
	UpsertServices(observations []ServiceObservation) error
	UpsertDependencies(observations []DependencyObservation) error
	ListServices(teamID int64) ([]ServiceRecord, error)
	GetService(teamID int64, serviceName string) (ServiceRecord, error)
	ListDependencies(teamID int64) ([]DependencyRecord, error)
	ListDependenciesForService(teamID int64, serviceName string) ([]DependencyRecord, error)
}

type BootstrapRepository interface {
	ListObservedServices(ctx context.Context) ([]ServiceObservation, error)
	ListObservedDependencies(ctx context.Context) ([]DependencyObservation, error)
}

type MySQLRepository struct {
	db dbutil.Querier
}

type ClickHouseBootstrapRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *sql.DB, appConfig registry.AppConfig) *MySQLRepository {
	return &MySQLRepository{
		db: dbutil.NewMySQLWrapper(
			db,
			appConfig.CircuitBreakerConsecutiveFailures(),
			appConfig.CircuitBreakerResetTimeout(),
		),
	}
}

func NewBootstrapRepository(db *dbutil.NativeQuerier) *ClickHouseBootstrapRepository {
	return &ClickHouseBootstrapRepository{db: db}
}

func (r *MySQLRepository) UpsertServices(observations []ServiceObservation) error {
	for _, item := range observations {
		if strings.TrimSpace(item.ServiceName) == "" {
			continue
		}
		firstSeenAt := coalesceTime(item.FirstSeenAt, item.LastSeenAt)
		lastSeenAt := coalesceTime(item.LastSeenAt, item.FirstSeenAt)
		_, err := r.db.Exec(`
			INSERT INTO service_inventory (
				team_id, service_name, display_name, owner_team, owner_name, on_call, tier,
				environment, runtime, language, repository_url, runbook_url, dashboard_url,
				service_type, cluster_name, tags, first_seen_at, last_seen_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				display_name = CASE WHEN VALUES(display_name) <> '' THEN VALUES(display_name) ELSE display_name END,
				owner_team = CASE WHEN owner_team = '' AND VALUES(owner_team) <> '' THEN VALUES(owner_team) ELSE owner_team END,
				owner_name = CASE WHEN owner_name = '' AND VALUES(owner_name) <> '' THEN VALUES(owner_name) ELSE owner_name END,
				on_call = CASE WHEN on_call = '' AND VALUES(on_call) <> '' THEN VALUES(on_call) ELSE on_call END,
				tier = CASE WHEN tier = '' AND VALUES(tier) <> '' THEN VALUES(tier) ELSE tier END,
				environment = CASE WHEN environment = '' AND VALUES(environment) <> '' THEN VALUES(environment) ELSE environment END,
				runtime = CASE WHEN runtime = '' AND VALUES(runtime) <> '' THEN VALUES(runtime) ELSE runtime END,
				language = CASE WHEN language = '' AND VALUES(language) <> '' THEN VALUES(language) ELSE language END,
				repository_url = CASE WHEN repository_url = '' AND VALUES(repository_url) <> '' THEN VALUES(repository_url) ELSE repository_url END,
				runbook_url = CASE WHEN runbook_url = '' AND VALUES(runbook_url) <> '' THEN VALUES(runbook_url) ELSE runbook_url END,
				dashboard_url = CASE WHEN dashboard_url = '' AND VALUES(dashboard_url) <> '' THEN VALUES(dashboard_url) ELSE dashboard_url END,
				service_type = CASE WHEN service_type = '' AND VALUES(service_type) <> '' THEN VALUES(service_type) ELSE service_type END,
				cluster_name = CASE WHEN cluster_name = '' AND VALUES(cluster_name) <> '' THEN VALUES(cluster_name) ELSE cluster_name END,
				tags = CASE WHEN JSON_LENGTH(tags) IS NULL OR JSON_LENGTH(tags) = 0 THEN VALUES(tags) ELSE tags END,
				first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
				last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at)),
				updated_at = CURRENT_TIMESTAMP
		`,
			item.TeamID,
			item.ServiceName,
			item.DisplayName,
			item.OwnerTeam,
			item.OwnerName,
			item.OnCall,
			item.Tier,
			item.Environment,
			item.Runtime,
			item.Language,
			item.RepositoryURL,
			item.RunbookURL,
			item.DashboardURL,
			item.ServiceType,
			item.ClusterName,
			dbutil.JSONString(item.Tags),
			firstSeenAt,
			lastSeenAt,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *MySQLRepository) UpsertDependencies(observations []DependencyObservation) error {
	for _, item := range observations {
		if strings.TrimSpace(item.SourceService) == "" || strings.TrimSpace(item.TargetService) == "" {
			continue
		}
		firstSeenAt := coalesceTime(item.FirstSeenAt, item.LastSeenAt)
		lastSeenAt := coalesceTime(item.LastSeenAt, item.FirstSeenAt)
		_, err := r.db.Exec(`
			INSERT INTO service_dependency_inventory (
				team_id, source_service, target_service, dependency_kind, first_seen_at, last_seen_at
			) VALUES (?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
				last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at)),
				updated_at = CURRENT_TIMESTAMP
		`,
			item.TeamID,
			item.SourceService,
			item.TargetService,
			firstNonEmpty(item.DependencyKind, "service"),
			firstSeenAt,
			lastSeenAt,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *MySQLRepository) ListServices(teamID int64) ([]ServiceRecord, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT team_id, service_name, display_name, owner_team, owner_name, on_call, tier,
		       environment, runtime, language, repository_url, runbook_url, dashboard_url,
		       service_type, cluster_name, tags, first_seen_at, last_seen_at
		FROM service_inventory
		WHERE team_id = ?
		ORDER BY service_name ASC
	`, teamID)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, serviceRecordFromMap(row))
	}
	return result, nil
}

func (r *MySQLRepository) GetService(teamID int64, serviceName string) (ServiceRecord, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT team_id, service_name, display_name, owner_team, owner_name, on_call, tier,
		       environment, runtime, language, repository_url, runbook_url, dashboard_url,
		       service_type, cluster_name, tags, first_seen_at, last_seen_at
		FROM service_inventory
		WHERE team_id = ? AND service_name = ?
		LIMIT 1
	`, teamID, serviceName)
	if err != nil {
		return ServiceRecord{}, err
	}
	if len(row) == 0 {
		return ServiceRecord{}, sql.ErrNoRows
	}
	return serviceRecordFromMap(row), nil
}

func (r *MySQLRepository) ListDependencies(teamID int64) ([]DependencyRecord, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT team_id, source_service, target_service, dependency_kind, first_seen_at, last_seen_at
		FROM service_dependency_inventory
		WHERE team_id = ?
		ORDER BY last_seen_at DESC, source_service ASC, target_service ASC
	`, teamID)
	if err != nil {
		return nil, err
	}
	result := make([]DependencyRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, dependencyRecordFromMap(row))
	}
	return result, nil
}

func (r *MySQLRepository) ListDependenciesForService(teamID int64, serviceName string) ([]DependencyRecord, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT team_id, source_service, target_service, dependency_kind, first_seen_at, last_seen_at
		FROM service_dependency_inventory
		WHERE team_id = ? AND (source_service = ? OR target_service = ?)
		ORDER BY last_seen_at DESC, source_service ASC, target_service ASC
	`, teamID, serviceName, serviceName)
	if err != nil {
		return nil, err
	}
	result := make([]DependencyRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, dependencyRecordFromMap(row))
	}
	return result, nil
}

type bootstrapServiceRow struct {
	TeamID      int64     `ch:"team_id"`
	ServiceName string    `ch:"service_name"`
	FirstSeenAt time.Time `ch:"first_seen_at"`
	LastSeenAt  time.Time `ch:"last_seen_at"`
	Environment string    `ch:"environment"`
	Runtime     string    `ch:"runtime"`
	Language    string    `ch:"language"`
	ClusterName string    `ch:"cluster_name"`
}

type bootstrapDependencyRow struct {
	TeamID         int64     `ch:"team_id"`
	SourceService  string    `ch:"source_service"`
	TargetService  string    `ch:"target_service"`
	DependencyKind string    `ch:"dependency_kind"`
	FirstSeenAt    time.Time `ch:"first_seen_at"`
	LastSeenAt     time.Time `ch:"last_seen_at"`
}

func (r *ClickHouseBootstrapRepository) ListObservedServices(ctx context.Context) ([]ServiceObservation, error) {
	var rows []bootstrapServiceRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(team_id) AS team_id,
		       service_name,
		       min(timestamp) AS first_seen_at,
		       max(timestamp) AS last_seen_at,
		       argMax(attributes.`+"`deployment.environment`"+`::String, timestamp) AS environment,
		       argMax(attributes.`+"`process.runtime.name`"+`::String, timestamp) AS runtime,
		       argMax(attributes.`+"`telemetry.sdk.language`"+`::String, timestamp) AS language,
		       argMax(attributes.`+"`k8s.cluster.name`"+`::String, timestamp) AS cluster_name
		FROM observability.spans
		WHERE service_name != ''
		GROUP BY team_id, service_name
	`)
	if err != nil {
		return nil, err
	}

	result := make([]ServiceObservation, 0, len(rows))
	for _, row := range rows {
		result = append(result, BuildDefaultServiceObservation(row.TeamID, row.ServiceName, row.LastSeenAt, map[string]string{
			"deployment.environment": row.Environment,
			"process.runtime.name":   row.Runtime,
			"telemetry.sdk.language": row.Language,
			"k8s.cluster.name":       row.ClusterName,
		}))
		if len(result) > 0 {
			result[len(result)-1].FirstSeenAt = row.FirstSeenAt.UTC()
		}
	}
	return result, nil
}

func (r *ClickHouseBootstrapRepository) ListObservedDependencies(ctx context.Context) ([]DependencyObservation, error) {
	var rows []bootstrapDependencyRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(s1.team_id) AS team_id,
		       s1.service_name AS source_service,
		       s2.service_name AS target_service,
		       'service' AS dependency_kind,
		       min(greatest(s1.timestamp, s2.timestamp)) AS first_seen_at,
		       max(greatest(s1.timestamp, s2.timestamp)) AS last_seen_at
		FROM observability.spans s1
		JOIN observability.spans s2
		  ON s1.team_id = s2.team_id
		 AND s1.trace_id = s2.trace_id
		 AND s1.span_id = s2.parent_span_id
		WHERE s1.kind = 3
		  AND s1.service_name != ''
		  AND s2.service_name != ''
		  AND s1.service_name != s2.service_name
		GROUP BY s1.team_id, s1.service_name, s2.service_name
	`)
	if err != nil {
		return nil, err
	}

	result := make([]DependencyObservation, 0, len(rows))
	for _, row := range rows {
		result = append(result, DependencyObservation{
			TeamID:         row.TeamID,
			SourceService:  row.SourceService,
			TargetService:  row.TargetService,
			DependencyKind: row.DependencyKind,
			FirstSeenAt:    row.FirstSeenAt.UTC(),
			LastSeenAt:     row.LastSeenAt.UTC(),
		})
	}
	return result, nil
}

func serviceRecordFromMap(row map[string]any) ServiceRecord {
	return ServiceRecord{
		TeamID:        dbutil.Int64FromAny(row["team_id"]),
		ServiceName:   dbutil.StringFromAny(row["service_name"]),
		DisplayName:   dbutil.StringFromAny(row["display_name"]),
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
		FirstSeenAt:   dbutil.StringFromAny(row["first_seen_at"]),
		LastSeenAt:    dbutil.StringFromAny(row["last_seen_at"]),
	}
}

func dependencyRecordFromMap(row map[string]any) DependencyRecord {
	return DependencyRecord{
		TeamID:         dbutil.Int64FromAny(row["team_id"]),
		SourceService:  dbutil.StringFromAny(row["source_service"]),
		TargetService:  dbutil.StringFromAny(row["target_service"]),
		DependencyKind: dbutil.StringFromAny(row["dependency_kind"]),
		FirstSeenAt:    dbutil.StringFromAny(row["first_seen_at"]),
		LastSeenAt:     dbutil.StringFromAny(row["last_seen_at"]),
	}
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

func coalesceTime(primary, fallback time.Time) time.Time {
	if !primary.IsZero() {
		return primary.UTC()
	}
	if !fallback.IsZero() {
		return fallback.UTC()
	}
	return time.Now().UTC()
}
