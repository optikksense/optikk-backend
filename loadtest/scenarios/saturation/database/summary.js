// Saturation / database / summary submodule (top-level KPIs).
// Route: GET /api/v1/saturation/database/summary

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';

export function dbSummary(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  client.get('/api/v1/saturation/database/summary',
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /saturation/database/summary' });
}
