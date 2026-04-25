// Infrastructure fleet: pod inventory.
// Endpoints:
//   GET /api/v1/infrastructure/fleet/pods

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';

export function infraFleet(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  client.get('/api/v1/infrastructure/fleet/pods',
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /infrastructure/fleet/pods' });
}
