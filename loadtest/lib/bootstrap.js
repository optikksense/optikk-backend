// Idempotent bootstrap: tries login first; on auth failure, walks the
// public team-create + user-create + login flow. Designed to be safe on a
// fresh local DB and a no-op on every subsequent run.

import http from 'k6/http';
import { cfg } from './config.js';
import { login, logout } from './auth.js';

const SAFE_HOST_PATTERNS = [
  /^http:\/\/localhost(:|\/|$)/i,
  /^http:\/\/127\.0\.0\.1(:|\/|$)/i,
  /^http:\/\/host\.docker\.internal(:|\/|$)/i,
  /(dev|staging|loadtest)/i,
];

export function assertSafeBaseUrl(baseUrl, allowRemote) {
  if (allowRemote) return;
  const safe = SAFE_HOST_PATTERNS.some((re) => re.test(baseUrl));
  if (!safe) {
    throw new Error(
      `bootstrap: refusing to create users on '${baseUrl}'. ` +
      `Set ALLOW_REMOTE_BOOTSTRAP=1 to override.`
    );
  }
}

function createTeam() {
  const res = http.post(
    `${cfg.baseUrl}/api/v1/teams`,
    JSON.stringify({
      team_name:   cfg.teamName,
      org_name:    cfg.orgName,
      description: 'Created by optikk-backend loadtest bootstrap',
    }),
    { headers: { 'Content-Type': 'application/json' }, tags: { module: 'bootstrap', endpoint: 'POST /teams' } }
  );
  if (res.status !== 200) {
    throw new Error(`bootstrap: createTeam failed status=${res.status} body=${res.body}`);
  }
  const body = res.json();
  if (!body || body.success !== true || !body.data || !body.data.id) {
    throw new Error(`bootstrap: createTeam returned invalid envelope body=${res.body}`);
  }
  return body.data.id;
}

function createUser(teamId) {
  const res = http.post(
    `${cfg.baseUrl}/api/v1/users`,
    JSON.stringify({
      email:    cfg.email,
      name:     'Loadtest User',
      password: cfg.password,
      role:     'member',
      teamIds:  [teamId],
    }),
    { headers: { 'Content-Type': 'application/json' }, tags: { module: 'bootstrap', endpoint: 'POST /users' } }
  );
  if (res.status !== 200) {
    throw new Error(`bootstrap: createUser failed status=${res.status} body=${res.body}`);
  }
}

export function ensureUser() {
  const first = login(cfg.email, cfg.password);
  if (first.ok) {
    console.info(`bootstrap: login ok userId=${first.userId} teams=${first.teamIds.join(',')}`);
    return { cookie: first.cookie, teamId: cfg.teamId || first.defaultTeamId, baseUrl: cfg.baseUrl };
  }

  console.info(`bootstrap: initial login failed (status=${first.status}) — attempting create flow`);
  assertSafeBaseUrl(cfg.baseUrl, cfg.allowRemoteBootstrap);

  const teamId = cfg.teamId || createTeam();
  if (!cfg.teamId) {
    console.info(`bootstrap: created team id=${teamId}`);
  } else {
    console.info(`bootstrap: using existing team id=${teamId}`);
  }

  createUser(teamId);
  console.info(`bootstrap: created user email=${cfg.email}`);

  const second = login(cfg.email, cfg.password);
  if (!second.ok) {
    throw new Error(`bootstrap: post-create login still failing status=${second.status} body=${second.body}`);
  }
  console.info(`bootstrap: login ok userId=${second.userId} teams=${second.teamIds.join(',')}`);
  return { cookie: second.cookie, teamId: cfg.teamId || second.defaultTeamId, baseUrl: cfg.baseUrl };
}

// Re-exported by entrypoints so k6 picks them up at the test-script top level.
export function setup() {
  return ensureUser();
}
export function teardown(ctx) {
  if (ctx && ctx.cookie) logout(ctx.cookie);
}
