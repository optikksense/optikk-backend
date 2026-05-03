// Infrastructure / jvm submodule.
// Routes (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/jvm/{memory,gc-duration,gc-collections,threads,
//                               classes,cpu,buffers}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const LEAVES = ['memory', 'gc-duration', 'gc-collections', 'threads', 'classes', 'cpu', 'buffers'];

export function infraJVM(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/infrastructure/jvm/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/jvm/${leaf}` });
  }
}
