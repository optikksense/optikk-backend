package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

func mysqlUp004(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`
CREATE TABLE IF NOT EXISTS observability.service_inventory
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id         BIGINT NOT NULL,
    service_name    VARCHAR(255) NOT NULL,
    display_name    VARCHAR(255) NOT NULL DEFAULT '',
    owner_team      VARCHAR(255) NOT NULL DEFAULT '',
    owner_name      VARCHAR(255) NOT NULL DEFAULT '',
    on_call         VARCHAR(255) NOT NULL DEFAULT '',
    tier            VARCHAR(64) NOT NULL DEFAULT '',
    environment     VARCHAR(128) NOT NULL DEFAULT '',
    runtime         VARCHAR(128) NOT NULL DEFAULT '',
    language        VARCHAR(128) NOT NULL DEFAULT '',
    repository_url  VARCHAR(512) NOT NULL DEFAULT '',
    runbook_url     VARCHAR(512) NOT NULL DEFAULT '',
    dashboard_url   VARCHAR(512) NOT NULL DEFAULT '',
    service_type    VARCHAR(64) NOT NULL DEFAULT '',
    cluster_name    VARCHAR(255) NOT NULL DEFAULT '',
    tags            JSON NULL,
    first_seen_at   DATETIME NOT NULL,
    last_seen_at    DATETIME NOT NULL,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_service_inventory_team_service (team_id, service_name),
    INDEX idx_service_inventory_team_last_seen (team_id, last_seen_at),
    INDEX idx_service_inventory_team_cluster (team_id, cluster_name)
)
`,
		`
CREATE TABLE IF NOT EXISTS observability.service_dependency_inventory
(
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id          BIGINT NOT NULL,
    source_service   VARCHAR(255) NOT NULL,
    target_service   VARCHAR(255) NOT NULL,
    dependency_kind  VARCHAR(64) NOT NULL DEFAULT 'service',
    first_seen_at    DATETIME NOT NULL,
    last_seen_at     DATETIME NOT NULL,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_service_dependency_inventory_team_edge (team_id, source_service, target_service, dependency_kind),
    INDEX idx_service_dependency_inventory_team_source (team_id, source_service),
    INDEX idx_service_dependency_inventory_team_target (team_id, target_service),
    INDEX idx_service_dependency_inventory_team_last_seen (team_id, last_seen_at)
)
`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

func mysqlDown004(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`DROP TABLE IF EXISTS observability.service_dependency_inventory`,
		`DROP TABLE IF EXISTS observability.service_inventory`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}
