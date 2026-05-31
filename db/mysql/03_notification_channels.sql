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
