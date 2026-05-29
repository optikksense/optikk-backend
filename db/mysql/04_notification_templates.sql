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
