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
