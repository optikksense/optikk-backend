// Cross-resource utilization views.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/resource-utilisation/{avg-cpu,avg-memory,
//          avg-network,avg-conn-pool,cpu-usage-percentage,
//          memory-usage-percentage,by-service,by-instance}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const PATHS = [
  'avg-cpu', 'avg-memory', 'avg-network', 'avg-conn-pool',
  'cpu-usage-percentage', 'memory-usage-percentage', 'by-service', 'by-instance',
];

export function infraResourceUtil(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/infrastructure/resource-utilisation/${seg}`,
      { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /infrastructure/resource-utilisation/${seg}` });
  }
}
