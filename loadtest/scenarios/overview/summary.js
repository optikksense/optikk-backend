// Overview summary tiles + headline charts (request-rate, error-rate,
// p95-latency, chart-metrics, services, top endpoints, summary, batch).
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/overview/{summary,batch-summary,chart-metrics,services,
//                     endpoints/metrics,error-rate,p95-latency,request-rate}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const PATHS = [
  '/api/v1/overview/summary',
  '/api/v1/overview/batch-summary',
  '/api/v1/overview/chart-metrics',
  '/api/v1/overview/services',
  '/api/v1/overview/endpoints/metrics',
  '/api/v1/overview/error-rate',
  '/api/v1/overview/p95-latency',
  '/api/v1/overview/request-rate',
];

export function overviewSummary(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const p of PATHS) {
    client.get(p, { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET ${p.replace('/api/v1', '')}` });
  }
}
