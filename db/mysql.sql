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

CREATE TABLE IF NOT EXISTS observability.dashboard_config_cleanup_backups
(
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id      BIGINT NOT NULL,
    removed_keys JSON NOT NULL,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dashboard_cleanup_team_id (team_id)
);

INSERT INTO observability.dashboard_config_cleanup_backups (team_id, removed_keys)
SELECT
    id,
    JSON_OBJECT(
        'ai-model-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."ai-model-detail"'),
        'database-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."database-detail"'),
        'error-group-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."error-group-detail"'),
        'kafka-group-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."kafka-group-detail"'),
        'kafka-topic-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."kafka-topic-detail"'),
        'node-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."node-detail"'),
        'operation-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."operation-detail"'),
        'redis-detail', JSON_EXTRACT(COALESCE(dashboard_configs, JSON_OBJECT()), '$."redis-detail"')
    )
FROM observability.teams;

UPDATE observability.teams
SET dashboard_configs = JSON_REMOVE(
    COALESCE(dashboard_configs, JSON_OBJECT()),
    '$."ai-model-detail"',
    '$."database-detail"',
    '$."error-group-detail"',
    '$."kafka-group-detail"',
    '$."kafka-topic-detail"',
    '$."node-detail"',
    '$."operation-detail"',
    '$."redis-detail"'
);
