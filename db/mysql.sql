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

-- LLM hub: scores, prompt registry, dataset snapshots, team pricing overrides (used by optikk-frontend /llm/*).
CREATE TABLE IF NOT EXISTS observability.llm_scores
  (
     id               BIGINT auto_increment PRIMARY KEY,
     team_id          BIGINT NOT NULL,
     name             VARCHAR(128) NOT NULL,
     value            DOUBLE NOT NULL,
     trace_id         VARCHAR(64) NOT NULL,
     span_id          VARCHAR(64) NOT NULL DEFAULT '',
     session_id       VARCHAR(512) NULL,
     prompt_template  VARCHAR(512) NULL,
     model            VARCHAR(256) NULL,
     source           VARCHAR(64) NOT NULL DEFAULT 'api',
     rationale        VARCHAR(2048) NULL,
     created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_llm_scores_team_time (team_id, created_at),
     INDEX idx_llm_scores_team_name (team_id, name),
     INDEX idx_llm_scores_trace (team_id, trace_id)
  );

CREATE TABLE IF NOT EXISTS observability.llm_prompts
  (
     id           BIGINT auto_increment PRIMARY KEY,
     team_id      BIGINT NOT NULL,
     slug         VARCHAR(128) NOT NULL,
     display_name VARCHAR(256) NOT NULL,
     body         MEDIUMTEXT NOT NULL,
     version      INT NOT NULL DEFAULT 1,
     created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at   DATETIME NULL,
     UNIQUE KEY uq_llm_prompt_team_slug (team_id, slug),
     INDEX idx_llm_prompts_team (team_id)
  );

CREATE TABLE IF NOT EXISTS observability.llm_datasets
  (
     id              BIGINT auto_increment PRIMARY KEY,
     team_id         BIGINT NOT NULL,
     name            VARCHAR(256) NOT NULL,
     query_snapshot  VARCHAR(2048) NULL,
     start_time_ms   BIGINT NOT NULL,
     end_time_ms     BIGINT NOT NULL,
     row_count       INT NOT NULL DEFAULT 0,
     payload_json    LONGTEXT NOT NULL,
     created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     INDEX idx_llm_datasets_team (team_id, created_at)
  );

-- LLM hub pricing overrides live on `teams.pricing_overrides_json` (see internal/modules/llm/hub).
-- The standalone table `llm_team_settings` was removed from this file; use the upgrade block below.

-- Alerting outbox: durable notification queue. Rows are written on every
-- state transition alongside the in-memory Dispatcher fast-path; the
-- OutboxRelay goroutine (internal/modules/alerting/outbox.go) delivers any
-- row still undelivered after the fast-lane window, so notifications survive
-- pod crashes and webhook outages.
CREATE TABLE IF NOT EXISTS observability.alert_outbox
  (
     id                BIGINT auto_increment PRIMARY KEY,
     team_id           BIGINT NOT NULL,
     alert_id          BIGINT NOT NULL,
     instance_key      VARCHAR(512) NOT NULL,
     transition_seq    BIGINT NOT NULL,
     payload_json      MEDIUMTEXT NOT NULL,
     attempts          INT NOT NULL DEFAULT 0,
     last_error        VARCHAR(1024) NULL,
     created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     next_attempt_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
     delivered_at      DATETIME NULL,
     UNIQUE KEY uq_alert_outbox_dedup (alert_id, instance_key, transition_seq),
     INDEX idx_alert_outbox_pending (delivered_at, next_attempt_at),
     INDEX idx_alert_outbox_team (team_id, created_at)
  );

-- =============================================================================
-- Manual upgrades (existing databases — run in order as needed)
-- =============================================================================
-- A) Add column if your `teams` row was created before `pricing_overrides_json` existed.
--    (Fails with "Duplicate column" if already applied — that is OK.)
--
--    ALTER TABLE observability.teams
--      ADD COLUMN pricing_overrides_json JSON NULL AFTER data_ingested_kb;
--
-- B) If you still have `llm_team_settings` from an older schema, copy data then drop:
--
--    UPDATE observability.teams t
--      INNER JOIN observability.llm_team_settings s ON t.id = s.team_id
--      SET t.pricing_overrides_json = s.pricing_overrides_json,
--          t.updated_at = UTC_TIMESTAMP();
--
--    DROP TABLE IF EXISTS observability.llm_team_settings;
