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
     retention_days           INT NOT NULL DEFAULT 30,
     data_ingested_kb         BIGINT NOT NULL DEFAULT 0,
     pricing_overrides_json   JSON NULL,
     created_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at               DATETIME NULL,
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

CREATE TABLE IF NOT EXISTS observability.saved_views
  (
     id            BIGINT auto_increment PRIMARY KEY,
     team_id       BIGINT NOT NULL,
     user_id       BIGINT NOT NULL,
     scope         VARCHAR(64) NOT NULL,
     name          VARCHAR(200) NOT NULL,
     url           VARCHAR(2000) NOT NULL,
     visibility    ENUM('private','team') NOT NULL DEFAULT 'private',
     created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at    DATETIME NULL,
     INDEX idx_saved_views_team_scope (team_id, scope),
     INDEX idx_saved_views_user (user_id)
  );
