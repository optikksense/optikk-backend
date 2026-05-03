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
        { field: 'service', op: 'eq', value: randomPick(services) }
      ],
      limit: 5
    }),
    { module: MOD, endpoint: 'POST /logs/query (discover)' },
  );

  const results = list && list.data && (list.data.results || list.data.logs || list.data.items);
  if (!Array.isArray(results) || results.length === 0) return;

  const log = results[0];
  const id = log.id || log.logId || log.log_id;
  if (!id) return;

  // 1. Test fetching log detail
  client.get(`/api/v1/logs/${encodeURIComponent(id)}`, null,
    { module: MOD, endpoint: 'GET /logs/:id' });

  // 2. Test trace_id index using a REAL trace ID from the discovered log
  const traceId = log.traceId || log.trace_id;
  if (traceId) {
    client.post('/api/v1/logs/query',
      logsQueryBody({
        ...w,
        filters: [{ field: 'trace_id', op: 'eq', value: traceId }],
        limit: 50
      }),
      { module: MOD, endpoint: 'POST /logs/query (by trace_id)' },
    );
  }
}
