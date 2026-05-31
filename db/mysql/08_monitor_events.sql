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
