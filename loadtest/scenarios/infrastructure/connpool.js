// Infrastructure / connpool submodule (DB-side connection pool gauges).
// Routes (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/connpool/{avg,by-service,by-instance}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const LEAVES = ['avg', 'by-service', 'by-instance'];

export function infraConnPool(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/infrastructure/connpool/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/connpool/${leaf}` });
  }
}
