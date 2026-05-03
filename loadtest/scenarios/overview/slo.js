// SLO overview: status, stats, burn-down, burn-rate.
// Endpoints:
//   GET /api/v1/slo
//   GET /api/v1/slo/stats
//   GET /api/v1/slo/burn-down
//   GET /api/v1/slo/burn-rate

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const PATHS = [
  '/api/v1/slo',
  '/api/v1/slo/stats',
  '/api/v1/slo/burn-down',
  '/api/v1/slo/burn-rate',
];

export function overviewSlo(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const p of PATHS) {
    client.get(p, { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET ${p.replace('/api/v1', '')}` });
  }
}
