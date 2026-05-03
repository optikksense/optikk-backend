// Infrastructure / cpu submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/cpu/{time,usage-percentage,load-average,
//                               process-count,avg,by-service,by-instance}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const LEAVES = ['time', 'usage-percentage', 'load-average', 'process-count', 'avg', 'by-service', 'by-instance'];

export function infraCPU(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/infrastructure/cpu/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/cpu/${leaf}` });
  }
}
