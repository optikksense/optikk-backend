// Traces Explorer: list query + peer facets + peer trend + drilldown.
// Endpoints exercised:
//   POST /api/v1/traces/query
//   POST /api/v1/traces/facets
//   POST /api/v1/traces/trend
//   GET  /api/v1/traces/:traceId

import { buildClient } from '../../lib/client.js';
import { tracesQueryBody, tracesFacetsBody, tracesTrendBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services, httpStatuses } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function tracesExplorer(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const filters = [
    { field: 'service',     op: 'eq', value: randomPick(services) },
    { field: 'http_status', op: 'eq', value: randomPick(httpStatuses) },
  ];

  const list = client.post('/api/v1/traces/query',
    tracesQueryBody({ ...w, filters, limit: 50 }),
    { module: MOD, endpoint: 'POST /traces/query' },
  );

  client.post('/api/v1/traces/facets',
    tracesFacetsBody({ ...w, filters }),
    { module: MOD, endpoint: 'POST /traces/facets' },
  );

  client.post('/api/v1/traces/trend',
    tracesTrendBody({ ...w, filters }),
    { module: MOD, endpoint: 'POST /traces/trend' },
  );

  const results = list && list.data && (list.data.results || list.data.traces || list.data.items);
  const first = Array.isArray(results) && results.length > 0 ? results[0] : null;
  const traceId = first && (first.traceId || first.trace_id || first.id);
  if (traceId) {
    client.get(`/api/v1/traces/${traceId}`, null, { module: MOD, endpoint: 'GET /traces/:traceId' });
  }
}
