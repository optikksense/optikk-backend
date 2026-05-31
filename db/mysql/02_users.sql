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
