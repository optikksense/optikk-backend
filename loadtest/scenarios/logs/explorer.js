// Logs Explorer: search.
// Endpoints:
//   POST /api/v1/logs/query

import { buildClient } from '../../lib/client.js';
import { logsQueryBody, logsFacetsBody, logsTrendsBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services, severities } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'logs';

export function logsExplorer(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  // Randomly test the trace_id lookup performance (~10% of the time)
  const isTraceLookup = Math.random() < 0.1;
  const traceIdFilter = isTraceLookup 
    ? [{ field: 'trace_id', op: 'eq', value: 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6' }] 
    : [];

  const baseFilters = [
    { field: 'service',       op: 'eq', value: randomPick(services) },
    { field: 'severity_text', op: 'eq', value: randomPick(severities) },
    ...traceIdFilter
  ];

  const list = client.post('/api/v1/logs/query',
    logsQueryBody({
      ...w,
      filters: baseFilters,
      limit: 100,
    }),
    { module: MOD, endpoint: 'POST /logs/query' },
  );

  const facets = client.post('/api/v1/logs/facets',
    logsFacetsBody({
      ...w,
      filters: baseFilters,
    }),
    { module: MOD, endpoint: 'POST /logs/facets' },
  );

  const trends = client.post('/api/v1/logs/trends',
    logsTrendsBody({
      ...w,
      filters: baseFilters,
      step: '1h',
    }),
    { module: MOD, endpoint: 'POST /logs/trends' },
  );
}
