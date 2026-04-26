// Database drilldown sub-modules under /api/v1/saturation/database/{group}/*.
// Endpoints (all GET, query: startTime+endTime):
//   /system/{latency,ops,top-collections-by-latency,top-collections-by-volume,
//            errors,namespaces}                                 — require db_system
//   /connections/{count,utilization,limits,pending,timeout-rate,
//                 wait-time,create-time,use-time}               — time only
//   /collection/{latency,ops,errors,query-texts,read-vs-write}  — require collection
//   /errors/{by-system,by-operation,by-error-type,by-collection,
//            by-status,ratio}                                   — time only
//   /latency/{by-system,by-operation,by-collection,by-namespace,
//             by-server,heatmap}                                — time only
//   /slow-queries/{patterns,collections,rate,p99-by-text}       — time only
//   /ops/{by-system,by-operation,by-collection,read-vs-write,by-namespace} — time only

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, datastoreSystems } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';

// Groups that require only time params
const TIME_ONLY_GROUPS = {
  connections:   ['count', 'utilization', 'limits', 'pending', 'timeout-rate', 'wait-time', 'create-time', 'use-time'],
  errors:        ['by-system', 'by-operation', 'by-error-type', 'by-collection', 'by-status', 'ratio'],
  latency:       ['by-system', 'by-operation', 'by-collection', 'by-namespace', 'by-server', 'heatmap'],
  'slow-queries':['patterns', 'collections', 'rate', 'p99-by-text'],
  ops:           ['by-system', 'by-operation', 'by-collection', 'read-vs-write', 'by-namespace'],
};

// /system/* endpoints require db_system query param
const SYSTEM_LEAVES = ['latency', 'ops', 'top-collections-by-latency', 'top-collections-by-volume', 'errors', 'namespaces'];

// /collection/* endpoints require collection query param
const COLLECTION_LEAVES = ['latency', 'ops', 'errors', 'query-texts', 'read-vs-write'];

// Sample collection names for the load test
const COLLECTIONS = ['users', 'orders', 'products', 'sessions', 'events'];

export function saturationDatastoresDrilldown(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  // Time-only groups: no extra params needed
  for (const group of Object.keys(TIME_ONLY_GROUPS)) {
    for (const leaf of TIME_ONLY_GROUPS[group]) {
      const path = `/api/v1/saturation/database/${group}/${leaf}`;
      client.get(path, q, { module: MOD, endpoint: `GET /saturation/database/${group}/${leaf}` });
    }
  }

  // /system/* requires db_system
  const dbSystem = randomPick(datastoreSystems);
  for (const leaf of SYSTEM_LEAVES) {
    const path = `/api/v1/saturation/database/system/${leaf}`;
    client.get(path, { ...q, db_system: dbSystem },
      { module: MOD, endpoint: `GET /saturation/database/system/${leaf}` });
  }

  // /collection/* requires collection
  const collection = randomPick(COLLECTIONS);
  for (const leaf of COLLECTION_LEAVES) {
    const path = `/api/v1/saturation/database/collection/${leaf}`;
    client.get(path, { ...q, collection },
      { module: MOD, endpoint: `GET /saturation/database/collection/${leaf}` });
  }
}
