-- ===========================================================================
-- MySQL Schema for Optikk Backend
-- ===========================================================================

-- -------------------------------------------
-- Users & Teams (core identity)
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    email            VARCHAR(255) NOT NULL UNIQUE,
    password_hash    VARCHAR(255),
    name             VARCHAR(255) NOT NULL DEFAULT '',
    avatar_url       VARCHAR(512),
    teams            JSON DEFAULT '[]',
    active           TINYINT(1) NOT NULL DEFAULT 1,
    oauth_provider   VARCHAR(50),
    oauth_id         VARCHAR(255),
    last_login_at    DATETIME,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_email (email),
    INDEX idx_oauth (oauth_provider, oauth_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS teams (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    org_name            VARCHAR(255) NOT NULL,
    name                VARCHAR(255) NOT NULL,
    slug                VARCHAR(255) NOT NULL,
    description         TEXT,
    active              TINYINT(1) NOT NULL DEFAULT 1,
    color               VARCHAR(20) DEFAULT '',
    icon                VARCHAR(100),
    api_key             VARCHAR(255) NOT NULL,
    slack_webhook_url   VARCHAR(512),
    dashboard_configs   JSON,
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE INDEX idx_org_slug (org_name, slug),
    INDEX idx_api_key (api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Saved Views
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS saved_views (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    created_by  BIGINT NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    view_type   VARCHAR(50) NOT NULL,
    query_json  TEXT NOT NULL,
    is_shared   TINYINT(1) NOT NULL DEFAULT 0,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team_user (team_id, created_by)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Annotations (Deployment Markers)
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS annotations (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id       BIGINT NOT NULL,
    created_by    BIGINT NOT NULL,
    title         VARCHAR(255) NOT NULL,
    description   TEXT DEFAULT '',
    timestamp     DATETIME NOT NULL,
    end_time      DATETIME,
    tags          VARCHAR(500) DEFAULT '',
    source        VARCHAR(50) NOT NULL DEFAULT 'manual',
    service_name  VARCHAR(255) DEFAULT '',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team_time (team_id, timestamp),
    INDEX idx_team_service (team_id, service_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Alerting: Rules
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS alert_rules (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id           BIGINT NOT NULL,
    created_by        BIGINT NOT NULL,
    name              VARCHAR(255) NOT NULL,
    description       TEXT DEFAULT '',
    enabled           TINYINT(1) NOT NULL DEFAULT 1,
    severity          VARCHAR(20) NOT NULL DEFAULT 'warning',
    condition_type    VARCHAR(20) NOT NULL DEFAULT 'threshold',
    signal_type       VARCHAR(20) NOT NULL DEFAULT 'span',
    query             TEXT NOT NULL,
    operator          VARCHAR(10) NOT NULL DEFAULT 'gt',
    threshold         DOUBLE NOT NULL DEFAULT 0,
    duration_minutes  INT NOT NULL DEFAULT 5,
    service_name      VARCHAR(255) DEFAULT '',
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team_enabled (team_id, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Alerting: Notification Channels
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS notification_channels (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id       BIGINT NOT NULL,
    created_by    BIGINT NOT NULL,
    name          VARCHAR(255) NOT NULL,
    channel_type  VARCHAR(20) NOT NULL,
    config        TEXT NOT NULL,
    enabled       TINYINT(1) NOT NULL DEFAULT 1,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team (team_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Alerting: Incidents
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS alert_incidents (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id          BIGINT NOT NULL,
    rule_id          BIGINT NOT NULL,
    rule_name        VARCHAR(255) NOT NULL,
    severity         VARCHAR(20) NOT NULL DEFAULT 'warning',
    status           VARCHAR(20) NOT NULL DEFAULT 'open',
    trigger_value    DOUBLE NOT NULL DEFAULT 0,
    threshold        DOUBLE NOT NULL DEFAULT 0,
    message          TEXT DEFAULT '',
    acknowledged_by  BIGINT,
    resolved_by      BIGINT,
    triggered_at     DATETIME NOT NULL,
    acknowledged_at  DATETIME,
    resolved_at      DATETIME,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_team_status (team_id, status),
    INDEX idx_team_rule (team_id, rule_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Custom Dashboards
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS custom_dashboards (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    created_by  BIGINT NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    is_shared   TINYINT(1) NOT NULL DEFAULT 0,
    layout_json TEXT,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team (team_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Dashboard Widgets
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS dashboard_widgets (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    dashboard_id  BIGINT NOT NULL,
    team_id       BIGINT NOT NULL,
    title         VARCHAR(255) NOT NULL,
    chart_type    VARCHAR(50) NOT NULL,
    query_json    TEXT NOT NULL,
    position_json TEXT NOT NULL,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_dashboard (dashboard_id, team_id),
    CONSTRAINT fk_widget_dashboard FOREIGN KEY (dashboard_id) REFERENCES custom_dashboards(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Errors Inbox (triage state)
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS error_group_status (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id       BIGINT NOT NULL,
    group_id      VARCHAR(64) NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'new',
    assigned_to   BIGINT,
    snoozed_until DATETIME,
    note          TEXT DEFAULT '',
    updated_by    BIGINT NOT NULL,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE INDEX idx_team_group (team_id, group_id),
    INDEX idx_team_status (team_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Shared Links
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS shared_links (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id        BIGINT NOT NULL,
    created_by     BIGINT NOT NULL,
    token          VARCHAR(64) NOT NULL,
    resource_type  VARCHAR(50) NOT NULL,
    resource_id    VARCHAR(255) NOT NULL,
    expires_at     DATETIME,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE INDEX idx_token (token),
    INDEX idx_team (team_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -------------------------------------------
-- Workloads (service grouping)
-- -------------------------------------------

CREATE TABLE IF NOT EXISTS workloads (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    team_id       BIGINT NOT NULL,
    created_by    BIGINT NOT NULL,
    name          VARCHAR(255) NOT NULL,
    description   TEXT DEFAULT '',
    service_names TEXT NOT NULL,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_team (team_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
