// Saturation / database / connections submodule (DB connection pool metrics).
// Routes (all GET, query: startTime+endTime):
//   /api/v1/saturation/database/connections/{count,utilization,limits,pending,
//                                            timeout-rate,wait-time,create-time,use-time}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['count', 'utilization', 'limits', 'pending', 'timeout-rate', 'wait-time', 'create-time', 'use-time'];

export function dbConnections(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/connections/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/connections/${leaf}` });
  }
}
