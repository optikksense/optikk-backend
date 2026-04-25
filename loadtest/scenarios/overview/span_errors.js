// Span-error analytics: hotspot, exception types, 5xx by route, latency
// breakdown.
// Endpoints:
//   GET /api/v1/spans/error-hotspot
//   GET /api/v1/spans/exception-rate-by-type
//   GET /api/v1/spans/http-5xx-by-route
//   GET /api/v1/spans/latency-breakdown

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const PATHS = [
  '/api/v1/spans/error-hotspot',
  '/api/v1/spans/exception-rate-by-type',
  '/api/v1/spans/http-5xx-by-route',
  '/api/v1/spans/latency-breakdown',
];

export function overviewSpanErrors(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const p of PATHS) {
    client.get(p, { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET ${p.replace('/api/v1', '')}` });
  }
}
