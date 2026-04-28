// Saturation / database / collection submodule. Requires ?collection=<name>.
// Routes (all GET, query: startTime+endTime+collection):
//   /api/v1/saturation/database/collection/{latency,ops,errors,query-texts,read-vs-write}

import { buildClient } from '../../../lib/client.js';
import { adaptiveWindow } from '../../../lib/timewindows.js';
import { randomPick } from '../../../lib/fixtures.js';
import { cfg } from '../../../lib/config.js';

const MOD = 'saturation';
const LEAVES = ['latency', 'ops', 'errors', 'query-texts', 'read-vs-write'];
const COLLECTIONS = ['users', 'orders', 'products', 'sessions', 'events'];

export function dbCollection(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime, collection: randomPick(COLLECTIONS) };
  for (const leaf of LEAVES) {
    client.get(`/api/v1/saturation/database/collection/${leaf}`, q,
      { module: MOD, endpoint: `GET /saturation/database/collection/${leaf}` });
  }
}
