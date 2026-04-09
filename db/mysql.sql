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

CREATE TABLE IF NOT EXISTS observability.alerts
  (
     id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
     team_id             BIGINT NOT NULL,
     name                VARCHAR(255) NOT NULL,
     description         TEXT,
     condition_type      VARCHAR(64) NOT NULL,
     target_ref          JSON NULL,
     group_by            JSON NULL,
     windows             JSON NULL,
     operator            VARCHAR(8) NOT NULL DEFAULT 'gt',
     warn_threshold      DOUBLE NULL,
     critical_threshold  DOUBLE NOT NULL DEFAULT 0,
     recovery_threshold  DOUBLE NULL,
     for_secs            BIGINT NOT NULL DEFAULT 0,
     recover_for_secs    BIGINT NOT NULL DEFAULT 0,
     keep_alive_secs     BIGINT NOT NULL DEFAULT 0,
     no_data_secs        BIGINT NOT NULL DEFAULT 0,
     severity            VARCHAR(8) NOT NULL DEFAULT 'p3',
     notify_template     TEXT,
     max_notifs_per_hour INT NOT NULL DEFAULT 0,
     slack_webhook_url   VARCHAR(1024),
     rule_state          VARCHAR(16) NOT NULL DEFAULT 'ok',
     last_eval_at        DATETIME NULL,
     instances           JSON NULL,
     mute_until          DATETIME NULL,
     silences            JSON NULL,
     enabled             TINYINT(1) NOT NULL DEFAULT 1,
     parent_alert_id     BIGINT NULL,
     created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     created_by          BIGINT NOT NULL DEFAULT 0,
     updated_by          BIGINT NOT NULL DEFAULT 0,
     INDEX idx_alerts_team_enabled (team_id, enabled),
     INDEX idx_alerts_rule_state (rule_state)
  );

CREATE TABLE IF NOT EXISTS observability.dashboard_config_cleanup_backups
(
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id      BIGINT NOT NULL,
    removed_keys JSON NOT NULL,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dashboard_cleanup_team_id (team_id)
);
