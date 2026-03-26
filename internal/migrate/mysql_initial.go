package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

// mysqlUp001 applies the initial MySQL schema (teams + users tables).
func mysqlUp001(ctx context.Context, db *sql.DB) error {
	for _, stmt := range splitStatements(mysqlDDL) {
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

// mysqlDown001 drops all tables created by the initial migration.
func mysqlDown001(ctx context.Context, db *sql.DB) error {
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS observability.users",
		"DROP TABLE IF EXISTS observability.teams",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

const mysqlDDL = `
CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.teams
  (
     id                BIGINT auto_increment PRIMARY KEY,
     org_name          VARCHAR(100) NOT NULL,
     name              VARCHAR(100) NOT NULL,
     slug              VARCHAR(50),
     description       VARCHAR(500),
     active            TINYINT(1) NOT NULL DEFAULT 1,
     color             VARCHAR(50),
     icon              VARCHAR(100),
     api_key           VARCHAR(64) NOT NULL UNIQUE,
     retention_days    INT NOT NULL DEFAULT 30,
     dashboard_configs JSON NULL,
     data_ingested_kb  BIGINT NOT NULL DEFAULT 0,
     created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at        DATETIME NULL,
     INDEX idx_team_api_key (api_key),
     UNIQUE KEY uq_team_org_name (org_name, name)
  );

CREATE TABLE IF NOT EXISTS observability.users
  (
     id            BIGINT auto_increment PRIMARY KEY,
     email         VARCHAR(255) NOT NULL UNIQUE,
     password_hash VARCHAR(255),
     name          VARCHAR(100) NOT NULL,
     avatar_url    VARCHAR(255),
     teams         JSON NOT NULL DEFAULT ('[]'),
     active        TINYINT(1) NOT NULL DEFAULT 1,
     last_login_at DATETIME NULL,
     created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_user_email (email)
  );
`
