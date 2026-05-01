// Logs By Trace: fetch all logs for a discovered trace_id.
// Discovers a real trace_id via /logs/query then hits the new
// /api/v1/logs/trace/:traceID endpoint backed by observability.trace_index.
// Endpoints:
//   GET /api/v1/logs/trace/:traceID

import { buildClient } from '../../lib/client.js';
import { logsQueryBody } from '../../lib/payloads.js';
import { windowFor } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'logs';

export function logsTraceLogs(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = windowFor(cfg.lookback);

  // 1. Discover a recent log row that has a non-empty trace_id.
  const list = client.post('/api/v1/logs/query',
    logsQueryBody({
      ...w,
      filters: [{ field: 'service', op: 'eq', value: randomPick(services) }],
      limit: 20,
    }),
    { module: MOD, endpoint: 'POST /logs/query (discover trace_id)' },
  );

  const results = list && list.data && (list.data.results || list.data.logs || list.data.items);
  if (!Array.isArray(results) || results.length === 0) return;

  // Pick the first result with a non-empty trace_id.
  let traceId = null;
  for (const r of results) {
    const t = r.traceId || r.trace_id;
    if (t && t.length > 0) { traceId = t; break; }
  }
  if (!traceId) return;

  // 2. Hit the canonical "all logs for a trace" endpoint.
  client.get(`/api/v1/logs/trace/${encodeURIComponent(traceId)}?limit=1000`, null,
    { module: MOD, endpoint: 'GET /logs/trace/:traceID' });
}
