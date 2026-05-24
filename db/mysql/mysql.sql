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

CREATE TABLE IF NOT EXISTS observability.monitors (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    team_id         BIGINT UNSIGNED NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    type            ENUM('metric', 'log', 'trace', 'composite') NOT NULL,
    query           JSON NOT NULL,
    thresholds      JSON NOT NULL,
    group_by        JSON,
    notification_channel_ids JSON,
    tags            JSON,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    on_missing_data ENUM('no_data', 'no_data_silent', 'resolve', 'evaluate_as_zero') NOT NULL DEFAULT 'no_data',
    evaluation_window_seconds INT UNSIGNED NOT NULL DEFAULT 300,
    evaluation_delay_seconds  INT UNSIGNED NOT NULL DEFAULT 0,
    min_request_threshold     INT UNSIGNED DEFAULT NULL,
    renotify_interval_minutes INT UNSIGNED DEFAULT NULL,
    created_by      BIGINT UNSIGNED NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_team_enabled (team_id, enabled),
    INDEX idx_type (type)
);

CREATE TABLE IF NOT EXISTS observability.monitor_states (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    monitor_id          BIGINT UNSIGNED NOT NULL,
    group_key           VARCHAR(512) NOT NULL DEFAULT '',
    current_state       ENUM('ok', 'warn', 'alert', 'no_data') NOT NULL DEFAULT 'ok',
    last_value          DOUBLE DEFAULT NULL,
    last_evaluated_at   TIMESTAMP NULL,
    last_triggered_at   TIMESTAMP NULL,
    last_resolved_at    TIMESTAMP NULL,
    last_notified_at    TIMESTAMP NULL,
    consecutive_breaches INT UNSIGNED NOT NULL DEFAULT 0,
    UNIQUE KEY uk_monitor_group (monitor_id, group_key),
    INDEX idx_state (current_state),
    FOREIGN KEY (monitor_id) REFERENCES observability.monitors(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observability.notification_channels (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    team_id     BIGINT UNSIGNED NOT NULL,
    name        VARCHAR(255) NOT NULL,
    type        ENUM('slack') NOT NULL,
    config      JSON NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_by  BIGINT UNSIGNED NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_team (team_id)
);

CREATE TABLE IF NOT EXISTS observability.alert_silences (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    team_id         BIGINT UNSIGNED NOT NULL,
    monitor_id      BIGINT UNSIGNED DEFAULT NULL,
    scope_tags      JSON,
    group_key_match VARCHAR(512) DEFAULT NULL,
    starts_at       TIMESTAMP NOT NULL,
    ends_at         TIMESTAMP DEFAULT NULL,
    reason          TEXT,
    recurring_cron  VARCHAR(100) DEFAULT NULL,
    recurring_duration_minutes INT UNSIGNED DEFAULT NULL,
    created_by      BIGINT UNSIGNED NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_team_active (team_id, starts_at, ends_at),
    INDEX idx_monitor (monitor_id)
);

CREATE TABLE IF NOT EXISTS observability.alert_events (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    team_id         BIGINT UNSIGNED NOT NULL,
    monitor_id      BIGINT UNSIGNED NOT NULL,
    group_key       VARCHAR(512) NOT NULL DEFAULT '',
    previous_state  ENUM('ok', 'warn', 'alert', 'no_data') NOT NULL,
    current_state   ENUM('ok', 'warn', 'alert', 'no_data') NOT NULL,
    evaluated_value DOUBLE DEFAULT NULL,
    threshold_value DOUBLE DEFAULT NULL,
    threshold_type  VARCHAR(50) DEFAULT NULL,
    message         TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_team_monitor_time (team_id, monitor_id, created_at),
    FOREIGN KEY (monitor_id) REFERENCES observability.monitors(id) ON DELETE CASCADE
);
