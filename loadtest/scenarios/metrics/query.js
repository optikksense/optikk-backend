// Metrics Explorer: timeseries query.
// Endpoints:
//   POST /api/v1/metrics/explorer/query
//
// Backend expects FEQueryRequest: { startTime, endTime, step, queries[] }
// Each query: { id, metricName, aggregation, where[], groupBy[] }

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, randomPickN, metricNames, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'metrics';
const AGGS = ['avg', 'sum', 'min', 'max'];

export function metricsQuery(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  const selectedMetrics = randomPickN(metricNames, 2);
  const selectedServices = randomPickN(services, 2);

  client.post('/api/v1/metrics/explorer/query',
    {
      startTime: w.startTime,
      endTime:   w.endTime,
      step:      '60s',
      queries:   selectedMetrics.map((name, i) => ({
        id:          `q${i}`,
        metricName:  name,
        aggregation: randomPick(AGGS),
        where: [{
          key:      'service.name',
          operator: 'in',
          value:    selectedServices,
        }],
        groupBy: [],
      })),
    },
    { module: MOD, endpoint: 'POST /metrics/explorer/query' },
  );
}
