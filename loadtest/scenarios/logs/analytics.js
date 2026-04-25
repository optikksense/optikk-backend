// Logs Analytics: group-by + aggregation queries.
// Endpoints:
//   POST /api/v1/logs/analytics

import { buildClient } from '../../lib/client.js';
import { logsAnalyticsBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'logs';

const VARIANTS = [
  {
    groupBy: ['severity_text'],
    aggregations: [{ fn: 'count', alias: 'count' }],
    vizMode: 'pie',
  },
  {
    groupBy: ['service', 'severity_text'],
    aggregations: [{ fn: 'count', alias: 'count' }],
    vizMode: 'table',
  },
  {
    groupBy: ['service'],
    aggregations: [
      { fn: 'count',          alias: 'count' },
      { fn: 'count_distinct', field: 'trace_id', alias: 'unique_traces' },
    ],
    vizMode: 'timeseries',
  },
];

export function logsAnalytics(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const v = randomPick(VARIANTS);

  client.post('/api/v1/logs/analytics',
    logsAnalyticsBody({
      ...w,
      filters: [{ field: 'service', op: 'eq', value: randomPick(services) }],
      ...v,
      step: '1m',
      limit: 50,
      orderBy: 'count desc',
    }),
    { module: MOD, endpoint: 'POST /logs/analytics' },
  );
}
