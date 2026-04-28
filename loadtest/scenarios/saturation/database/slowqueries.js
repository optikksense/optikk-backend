// Saturation / database / slow-queries submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/saturation/database/slow-queries/{patterns,collections,rate,p99-by-text}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['patterns', 'collections', 'rate', 'p99-by-text'];

export function dbSlowQueries(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/slow-queries/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/slow-queries/${leaf}` });
  }
}
