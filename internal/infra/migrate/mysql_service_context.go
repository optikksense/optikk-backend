package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

func mysqlUp003(ctx context.Context, db *sql.DB) error {
	for _, stmt := range splitStatements(mysqlServiceContextDDL) {
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

func mysqlDown003(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS observability.service_change_events",
		"DROP TABLE IF EXISTS observability.service_incidents",
		"DROP TABLE IF EXISTS observability.service_deployments",
		"DROP TABLE IF EXISTS observability.service_catalog_entries",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

const mysqlServiceContextDDL = `
CREATE TABLE IF NOT EXISTS observability.service_catalog_entries
  (
     id              BIGINT auto_increment PRIMARY KEY,
     team_id         BIGINT NOT NULL,
     service_name    VARCHAR(255) NOT NULL,
     display_name    VARCHAR(255) NOT NULL,
     description     VARCHAR(500),
     owner_team      VARCHAR(100) NOT NULL DEFAULT '',
     owner_name      VARCHAR(100) NOT NULL DEFAULT '',
     on_call         VARCHAR(100) NOT NULL DEFAULT '',
     tier            VARCHAR(32) NOT NULL DEFAULT '',
     environment     VARCHAR(32) NOT NULL DEFAULT 'production',
     runtime         VARCHAR(64) NOT NULL DEFAULT '',
     language        VARCHAR(64) NOT NULL DEFAULT '',
     repository_url  VARCHAR(255),
     runbook_url     VARCHAR(255),
     dashboard_url   VARCHAR(255),
     service_type    VARCHAR(32) NOT NULL DEFAULT '',
     cluster_name    VARCHAR(100) NOT NULL DEFAULT '',
     tags            JSON NULL,
     created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     UNIQUE KEY uq_service_catalog_team_service (team_id, service_name),
     INDEX idx_service_catalog_team_owner (team_id, owner_team),
     INDEX idx_service_catalog_team_cluster (team_id, cluster_name)
  );

CREATE TABLE IF NOT EXISTS observability.service_deployments
  (
     id              BIGINT auto_increment PRIMARY KEY,
     team_id         BIGINT NOT NULL,
     service_name    VARCHAR(255) NOT NULL,
     version         VARCHAR(100) NOT NULL,
     environment     VARCHAR(32) NOT NULL DEFAULT 'production',
     status          VARCHAR(32) NOT NULL,
     summary         VARCHAR(500),
     deployed_by     VARCHAR(100),
     commit_sha      VARCHAR(64),
     started_at      DATETIME NOT NULL,
     finished_at     DATETIME NULL,
     change_summary  VARCHAR(500),
     created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_service_deployments_team_service_time (team_id, service_name, started_at),
     INDEX idx_service_deployments_team_time (team_id, started_at)
  );

CREATE TABLE IF NOT EXISTS observability.service_incidents
  (
     id              BIGINT auto_increment PRIMARY KEY,
     team_id         BIGINT NOT NULL,
     service_name    VARCHAR(255) NOT NULL,
     title           VARCHAR(255) NOT NULL,
     severity        VARCHAR(32) NOT NULL,
     status          VARCHAR(32) NOT NULL,
     summary         VARCHAR(500),
     commander       VARCHAR(100),
     started_at      DATETIME NOT NULL,
     resolved_at     DATETIME NULL,
     created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_service_incidents_team_service_time (team_id, service_name, started_at),
     INDEX idx_service_incidents_team_status (team_id, status)
  );

CREATE TABLE IF NOT EXISTS observability.service_change_events
  (
     id                 BIGINT auto_increment PRIMARY KEY,
     team_id            BIGINT NOT NULL,
     service_name       VARCHAR(255) NOT NULL,
     event_type         VARCHAR(32) NOT NULL,
     title              VARCHAR(255) NOT NULL,
     summary            VARCHAR(500),
     related_reference  VARCHAR(255),
     happened_at        DATETIME NOT NULL,
     created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_service_change_events_team_service_time (team_id, service_name, happened_at)
  );
`
