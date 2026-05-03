// Saturation / database / errors submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/saturation/database/errors/{by-system,by-operation,by-error-type,
//                                       by-collection,by-status,ratio}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['by-system', 'by-operation', 'by-error-type', 'by-collection', 'by-status', 'ratio'];

export function dbErrors(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/errors/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/errors/${leaf}` });
  }
}
