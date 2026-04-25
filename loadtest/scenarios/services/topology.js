// Service topology graph.
// Endpoints:
//   GET /api/v1/services/topology

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'services';

export function servicesTopology(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  client.get('/api/v1/services/topology',
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /services/topology' });
}
