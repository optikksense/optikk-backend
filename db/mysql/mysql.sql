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

-- Alerting / monitors platform ------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.notification_channels
  (
     id                BIGINT AUTO_INCREMENT PRIMARY KEY,
     team_id           BIGINT NOT NULL,
     type              ENUM('slack','pagerduty','opsgenie','teams','email','webhook','jira') NOT NULL,
     name              VARCHAR(200) NOT NULL,
     config_json       JSON NOT NULL,
     status            ENUM('ok','warn','muted') NOT NULL DEFAULT 'ok',
     last_used_at      DATETIME NULL,
     last_delivery_at  DATETIME NULL,
     last_error_text   VARCHAR(500) NULL,
     created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at        DATETIME NULL,
     INDEX idx_nc_team (team_id)
  );

CREATE TABLE IF NOT EXISTS observability.notification_templates
  (
     id           BIGINT AUTO_INCREMENT PRIMARY KEY,
     team_id      BIGINT NOT NULL,
     name         VARCHAR(200) NOT NULL,
     description  VARCHAR(500),
     body         TEXT NOT NULL,
     used_count   INT NOT NULL DEFAULT 0,
     created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at   DATETIME NULL,
     INDEX idx_nt_team (team_id)
  );

CREATE TABLE IF NOT EXISTS observability.notification_policies
  (
     id            BIGINT AUTO_INCREMENT PRIMARY KEY,
     team_id       BIGINT NOT NULL,
     name          VARCHAR(200) NOT NULL,
     match_dsl     VARCHAR(500) NOT NULL,
     actions_json  JSON NOT NULL,
     hits_30d      INT NOT NULL DEFAULT 0,
     last_used_at  DATETIME NULL,
     enabled       TINYINT(1) NOT NULL DEFAULT 1,
     position      INT NOT NULL DEFAULT 0,
     created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at    DATETIME NULL,
     INDEX idx_np_team_position (team_id, position)
  );

CREATE TABLE IF NOT EXISTS observability.monitors
  (
     id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
     team_id              BIGINT NOT NULL,
     name                 VARCHAR(300) NOT NULL,
     type                 ENUM('metric','apm','log') NOT NULL,
     priority             ENUM('P1','P2','P3','P4') NOT NULL DEFAULT 'P2',
     scope_json           JSON NOT NULL,
     query_json           JSON NOT NULL,
     conditions_json      JSON NOT NULL,
     notify_json          JSON NOT NULL,
     message_template_id  BIGINT NULL,
     message_body         TEXT NULL,
     runbook_url          VARCHAR(500) NULL,
     tags_json            JSON NOT NULL DEFAULT ('[]'),
     eval_every_sec       INT NOT NULL DEFAULT 300,
     renotify_every_sec   INT NULL,
     muted_until          DATETIME NULL,
     active               TINYINT(1) NOT NULL DEFAULT 1,
     created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at           DATETIME NULL,
     created_by_user_id   BIGINT NULL,
     INDEX idx_m_team_active (team_id, active),
     INDEX idx_m_team_muted (team_id, muted_until),
     INDEX idx_m_team_priority (team_id, priority)
  );

CREATE TABLE IF NOT EXISTS observability.monitor_state
  (
     monitor_id           BIGINT PRIMARY KEY,
     status               ENUM('alert','warn','ok','no_data') NOT NULL DEFAULT 'no_data',
     current_value        DOUBLE NULL,
     last_evaluated_at    DATETIME NULL,
     next_evaluation_at   DATETIME NOT NULL,
     triggered_at         DATETIME NULL,
     last_notified_at     DATETIME NULL,
     evaluation_count     BIGINT NOT NULL DEFAULT 0,
     acked_by_user_id     BIGINT NULL,
     acked_at             DATETIME NULL,
     INDEX idx_ms_next (next_evaluation_at)
  );

CREATE TABLE IF NOT EXISTS observability.monitor_events
  (
     id           BIGINT AUTO_INCREMENT PRIMARY KEY,
     monitor_id   BIGINT NOT NULL,
     team_id      BIGINT NOT NULL,
     kind         ENUM('triggered','recovered','acked','muted','test') NOT NULL,
     value        DOUBLE NULL,
     threshold    DOUBLE NULL,
     started_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     ended_at     DATETIME NULL,
     resolved_by  VARCHAR(200) NULL,
     peak_value   DOUBLE NULL,
     note         VARCHAR(500) NULL,
     INDEX idx_me_monitor (monitor_id, started_at),
     INDEX idx_me_team (team_id, started_at)
  );
