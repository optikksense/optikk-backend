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
