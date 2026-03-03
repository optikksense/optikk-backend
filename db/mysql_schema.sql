CREATE TABLE IF NOT EXISTS users (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255),
  name VARCHAR(100) NOT NULL,
  avatar_url VARCHAR(255),
  role VARCHAR(20) NOT NULL DEFAULT 'member',
  teams JSON NOT NULL DEFAULT ('[]'),
  active TINYINT(1) NOT NULL DEFAULT 1,
  last_login_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NULL,
  INDEX idx_user_email (email)
);

CREATE TABLE IF NOT EXISTS teams (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  organization_id BIGINT NOT NULL,
  org_name VARCHAR(100),
  name VARCHAR(100) NOT NULL,
  slug VARCHAR(50),
  description VARCHAR(500),
  active TINYINT(1) NOT NULL DEFAULT 1,
  color VARCHAR(50),
  icon VARCHAR(100),
  api_key VARCHAR(64) UNIQUE,
  retention_days INT NOT NULL DEFAULT 30,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NULL,
  INDEX idx_team_org (organization_id),
  INDEX idx_team_slug (slug),
  INDEX idx_team_api_key (api_key)
);


