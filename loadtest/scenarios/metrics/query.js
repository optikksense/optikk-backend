// Metrics Explorer: timeseries query.
// Endpoints:
//   POST /api/v1/metrics/explorer/query

import { buildClient } from '../../lib/client.js';
import { metricsQueryBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, randomPickN, metricNames, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'metrics';
const AGGS = ['avg', 'sum', 'min', 'max'];

export function metricsQuery(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  client.post('/api/v1/metrics/explorer/query',
    metricsQueryBody({
      metricNames: randomPickN(metricNames, 2),
      tagFilters:  { 'service.name': randomPickN(services, 2) },
      aggregation: randomPick(AGGS),
      ...w,
      step: '60s',
    }),
    { module: MOD, endpoint: 'POST /metrics/explorer/query' },
  );
}
