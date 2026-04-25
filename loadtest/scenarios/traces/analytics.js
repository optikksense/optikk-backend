// Traces Analytics: group-by + aggregation queries.
// Endpoints exercised:
//   POST /api/v1/traces/analytics

import { buildClient } from '../../lib/client.js';
import { tracesAnalyticsBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

const VARIANTS = [
  {
    groupBy: ['service'],
    aggregations: [{ fn: 'count', alias: 'count' }],
    vizMode: 'timeseries',
  },
  {
    groupBy: ['service', 'operation'],
    aggregations: [
      { fn: 'count', alias: 'count' },
      { fn: 'p95',   field: 'duration_ms', alias: 'p95' },
    ],
    vizMode: 'table',
  },
  {
    groupBy: ['http_status'],
    aggregations: [{ fn: 'count', alias: 'count' }],
    vizMode: 'pie',
  },
];

export function tracesAnalytics(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const variant = randomPick(VARIANTS);

  client.post('/api/v1/traces/analytics',
    tracesAnalyticsBody({
      ...w,
      filters: [{ field: 'service', op: 'eq', value: randomPick(services) }],
      ...variant,
      step: '1m',
      limit: 50,
      orderBy: 'count desc',
    }),
    { module: MOD, endpoint: 'POST /traces/analytics' },
  );
}
