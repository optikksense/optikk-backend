// RED metrics (Rate / Errors / Duration) under /api/v1/spans/red/*.
// Endpoints:
//   GET /api/v1/spans/red/{summary,apdex,top-slow-operations,
//                          top-error-operations,request-rate,error-rate,
//                          p95-latency,span-kind-breakdown,errors-by-route}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const PATHS = [
  '/api/v1/spans/red/summary',
  '/api/v1/spans/red/apdex',
  '/api/v1/spans/red/top-slow-operations',
  '/api/v1/spans/red/top-error-operations',
  '/api/v1/spans/red/request-rate',
  '/api/v1/spans/red/error-rate',
  '/api/v1/spans/red/p95-latency',
  '/api/v1/spans/red/span-kind-breakdown',
  '/api/v1/spans/red/errors-by-route',
];

export function overviewRedMetrics(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const p of PATHS) {
    client.get(p, { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET ${p.replace('/api/v1', '')}` });
  }
}
