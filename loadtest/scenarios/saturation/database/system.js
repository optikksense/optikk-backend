// Saturation / database / system submodule. Requires ?db_system=<name>.
// Routes (all GET, query: startTime+endTime+db_system):
//   /api/v1/saturation/database/system/{latency,ops,top-collections-by-latency,
//                                       top-collections-by-volume,errors,namespaces}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { randomPick, datastoreSystems } from '../../../lib/fixtures.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['latency', 'ops', 'top-collections-by-latency', 'top-collections-by-volume', 'errors', 'namespaces'];

export function dbSystem(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime, db_system: randomPick(datastoreSystems) };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/system/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/system/${leaf}` });
  }
}
