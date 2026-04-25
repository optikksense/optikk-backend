// Pure auth primitives. login() returns the optikk_session cookie value
// extracted from the Set-Cookie header so subsequent requests can attach it
// directly. Used by both bootstrap.js and entrypoint setup() functions.

import http from 'k6/http';
import { cfg } from './config.js';

const SESSION_COOKIE = 'optikk_session';

function extractSessionCookie(res) {
  const cookies = res.cookies || {};
  const arr = cookies[SESSION_COOKIE];
  if (arr && arr.length > 0 && arr[0].value) {
    return arr[0].value;
  }
  const setCookie = (res.headers && (res.headers['Set-Cookie'] || res.headers['set-cookie'])) || '';
  const match = setCookie.match(new RegExp(`${SESSION_COOKIE}=([^;]+)`));
  return match ? match[1] : '';
}

export function login(email, password) {
  const url = `${cfg.baseUrl}/api/v1/auth/login`;
  const res = http.post(url, JSON.stringify({ email, password }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { module: 'auth', endpoint: 'POST /auth/login' },
  });

  if (res.status !== 200) {
    return { ok: false, status: res.status, body: res.body };
  }

  let body;
  try { body = res.json(); } catch (_e) { return { ok: false, status: 200, body: 'unparseable JSON' }; }
  if (!body || body.success !== true || !body.data) {
    return { ok: false, status: 200, body: res.body };
  }

  const cookie = extractSessionCookie(res);
  if (!cookie) {
    return { ok: false, status: 200, body: 'no session cookie returned' };
  }

  const data = body.data;
  return {
    ok: true,
    cookie,
    userId:        data.user && data.user.id,
    teamIds:       (data.teams || []).map((t) => t.id),
    defaultTeamId: data.currentTeam && data.currentTeam.id,
  };
}

export function logout(cookie) {
  if (!cookie) return;
  http.post(`${cfg.baseUrl}/api/v1/auth/logout`, null, {
    headers: { Cookie: `${SESSION_COOKIE}=${cookie}` },
    tags: { module: 'auth', endpoint: 'POST /auth/logout' },
  });
}
