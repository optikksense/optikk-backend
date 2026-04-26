// HTTP wrapper used by every scenario. Closures over the session cookie +
// team id so scenario iterations stay terse and well-tagged. Returns the
// parsed envelope body (or null on failure) so callers can chain follow-up
// requests against a real id.

import http from 'k6/http';
import { assertOk } from './checks.js';
import { reqDuration } from './metrics.js';
import { cfg } from './config.js';

const SESSION_COOKIE = 'optikk_session';

function buildHeaders(ctx) {
  const headers = {
    'Content-Type': 'application/json',
    'Cookie':       `${SESSION_COOKIE}=${ctx.cookie}`,
  };
  if (ctx.teamId) headers['X-Team-Id'] = String(ctx.teamId);
  if (cfg.bypassCache) {
    headers['Cache-Control'] = 'no-cache';
    headers['X-Optikk-Bypass-Cache'] = '1';
  }
  return headers;
}

function recordDuration(res, tags) {
  if (res && Number.isFinite(res.timings && res.timings.duration)) {
    reqDuration.add(res.timings.duration, tags);
  }
}

function buildUrl(baseUrl, path, query) {
  if (!query) return `${baseUrl}${path}`;
  const parts = [];
  for (const k of Object.keys(query)) {
    if (query[k] === undefined || query[k] === null) continue;
    parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(query[k])}`);
  }
  return parts.length ? `${baseUrl}${path}?${parts.join('&')}` : `${baseUrl}${path}`;
}

export function buildClient(ctx) {
  const headers = buildHeaders(ctx);
  return {
    get(path, query, tags) {
      const t = { ...(tags || {}) };
      const res = http.get(buildUrl(ctx.baseUrl, path, query), { headers, tags: t });
      recordDuration(res, t);
      return assertOk(res, t);
    },
    post(path, body, tags) {
      const t = { ...(tags || {}) };
      const payload = body == null ? '' : (typeof body === 'string' ? body : JSON.stringify(body));
      const res = http.post(`${ctx.baseUrl}${path}`, payload, { headers, tags: t });
      recordDuration(res, t);
      return assertOk(res, t);
    },
  };
}
