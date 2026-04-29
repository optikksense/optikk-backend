// Logs Detail: fetch a single log row.
// Discovers via /logs/query then fetches /logs/:id.
// Endpoints:
//   GET /api/v1/logs/:id

import { buildClient } from '../../lib/client.js';
import { logsQueryBody } from '../../lib/payloads.js';
import { windowFor } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'logs';

export function logsDetail(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = windowFor(cfg.lookback);

  const list = client.post('/api/v1/logs/query',
    logsQueryBody({
      ...w,
      filters: [
        { field: 'service', op: 'eq', value: randomPick(services) },
        // A mock trace ID to test the trace_id filtering logic on the index without matching rows
        { field: 'trace_id', op: 'eq', value: 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6' }
      ],
      include: ['summary'],
      limit: 5
    }),
    { module: MOD, endpoint: 'POST /logs/query (discover)' },
  );

  const results = list && list.data && (list.data.results || list.data.logs || list.data.items);
  if (!Array.isArray(results) || results.length === 0) return;
  const id = results[0].id || results[0].logId || results[0].log_id;
  if (!id) return;

  client.get(`/api/v1/logs/${encodeURIComponent(id)}`, null,
    { module: MOD, endpoint: 'GET /logs/:id' });
}
