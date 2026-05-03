// Saturation / database / systems submodule (detected DB systems list).
// Route: GET /api/v1/saturation/database/systems

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';

export function dbSystems(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  client.get('/api/v1/saturation/database/systems',
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /saturation/database/systems' });
}
