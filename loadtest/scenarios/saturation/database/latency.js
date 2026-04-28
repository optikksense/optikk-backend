// Saturation / database / latency submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/saturation/database/latency/{by-system,by-operation,by-collection,
//                                        by-namespace,by-server,heatmap}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['by-system', 'by-operation', 'by-collection', 'by-namespace', 'by-server', 'heatmap'];

export function dbLatency(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/latency/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/latency/${leaf}` });
  }
}
