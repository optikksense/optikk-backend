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

CREATE TABLE IF NOT EXISTS observability.ai_provider_secrets
(
    id              VARCHAR(64) PRIMARY KEY,
    team_id         BIGINT NOT NULL,
    provider        VARCHAR(64) NOT NULL,
    key_label       VARCHAR(128) NOT NULL,
    encrypted_value TEXT NOT NULL,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ai_provider_secret (team_id, provider, key_label),
    INDEX idx_ai_provider_team (team_id)
);

CREATE TABLE IF NOT EXISTS observability.ai_prompts
(
    id                VARCHAR(64) PRIMARY KEY,
    team_id           BIGINT NOT NULL,
    name              VARCHAR(255) NOT NULL,
    slug              VARCHAR(255) NOT NULL,
    description       TEXT NULL,
    model_provider    VARCHAR(128) NOT NULL,
    model_name        VARCHAR(255) NOT NULL,
    tags_json         JSON NULL,
    latest_version    INT NOT NULL DEFAULT 1,
    active_version_id VARCHAR(64) NULL,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ai_prompt_slug (team_id, slug),
    INDEX idx_ai_prompt_team_updated (team_id, updated_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_prompt_versions
(
    id             VARCHAR(64) PRIMARY KEY,
    prompt_id      VARCHAR(64) NOT NULL,
    version_number INT NOT NULL,
    changelog      TEXT NULL,
    system_prompt  MEDIUMTEXT NOT NULL,
    user_template  MEDIUMTEXT NOT NULL,
    variables_json JSON NULL,
    is_active      TINYINT(1) NOT NULL DEFAULT 0,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ai_prompt_version (prompt_id, version_number),
    INDEX idx_ai_prompt_versions_prompt (prompt_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_datasets
(
    id          VARCHAR(64) PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT NULL,
    tags_json   JSON NULL,
    item_count  INT NOT NULL DEFAULT 0,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_dataset_team_updated (team_id, updated_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_dataset_items
(
    id              VARCHAR(64) PRIMARY KEY,
    dataset_id      VARCHAR(64) NOT NULL,
    input_json      JSON NOT NULL,
    expected_output MEDIUMTEXT NULL,
    metadata_json   JSON NULL,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_dataset_items_dataset (dataset_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_feedback
(
    id          VARCHAR(64) PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_id   VARCHAR(64) NOT NULL,
    run_span_id VARCHAR(64) NULL,
    trace_id    VARCHAR(64) NULL,
    score       INT NOT NULL DEFAULT 0,
    label       VARCHAR(128) NOT NULL,
    comment     TEXT NULL,
    created_by  VARCHAR(255) NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_feedback_target (team_id, target_type, target_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_eval_suites
(
    id          VARCHAR(64) PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT NULL,
    prompt_id   VARCHAR(64) NOT NULL,
    dataset_id  VARCHAR(64) NOT NULL,
    judge_model VARCHAR(255) NOT NULL,
    status      VARCHAR(32) NOT NULL DEFAULT 'draft',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_eval_team_updated (team_id, updated_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_eval_runs
(
    id                VARCHAR(64) PRIMARY KEY,
    team_id           BIGINT NOT NULL,
    eval_id           VARCHAR(64) NOT NULL,
    prompt_version_id VARCHAR(64) NOT NULL,
    dataset_id        VARCHAR(64) NOT NULL,
    status            VARCHAR(32) NOT NULL DEFAULT 'queued',
    average_score     DOUBLE NOT NULL DEFAULT 0,
    pass_rate         DOUBLE NOT NULL DEFAULT 0,
    total_cases       INT NOT NULL DEFAULT 0,
    completed_cases   INT NOT NULL DEFAULT 0,
    summary_json      JSON NULL,
    started_at        DATETIME NULL,
    finished_at       DATETIME NULL,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_eval_runs_queue (status, created_at),
    INDEX idx_ai_eval_runs_eval (team_id, eval_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_eval_scores
(
    id              VARCHAR(64) PRIMARY KEY,
    eval_run_id     VARCHAR(64) NOT NULL,
    dataset_item_id VARCHAR(64) NOT NULL,
    score           DOUBLE NOT NULL DEFAULT 0,
    result_label    VARCHAR(32) NOT NULL,
    reason          TEXT NULL,
    output_text     MEDIUMTEXT NULL,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_eval_scores_run (eval_run_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_experiments
(
    id          VARCHAR(64) PRIMARY KEY,
    team_id     BIGINT NOT NULL,
    name        VARCHAR(255) NOT NULL,
    description TEXT NULL,
    dataset_id  VARCHAR(64) NOT NULL,
    status      VARCHAR(32) NOT NULL DEFAULT 'draft',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_experiment_team_updated (team_id, updated_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_experiment_variants
(
    id                VARCHAR(64) PRIMARY KEY,
    experiment_id     VARCHAR(64) NOT NULL,
    prompt_version_id VARCHAR(64) NOT NULL,
    label             VARCHAR(255) NOT NULL,
    weight            DOUBLE NOT NULL DEFAULT 1,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_experiment_variants_exp (experiment_id, created_at)
);

CREATE TABLE IF NOT EXISTS observability.ai_experiment_runs
(
    id                VARCHAR(64) PRIMARY KEY,
    team_id           BIGINT NOT NULL,
    experiment_id     VARCHAR(64) NOT NULL,
    status            VARCHAR(32) NOT NULL DEFAULT 'queued',
    winner_variant_id VARCHAR(64) NULL,
    summary_json      JSON NULL,
    started_at        DATETIME NULL,
    finished_at       DATETIME NULL,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_experiment_runs_queue (status, created_at),
    INDEX idx_ai_experiment_runs_experiment (team_id, experiment_id, created_at)
);
