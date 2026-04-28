// Saturation / database / volume submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/saturation/database/ops/{by-system,by-operation,by-collection,
//                                    read-vs-write,by-namespace}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['by-system', 'by-operation', 'by-collection', 'read-vs-write', 'by-namespace'];

export function dbVolume(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/ops/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/ops/${leaf}` });
  }
}
