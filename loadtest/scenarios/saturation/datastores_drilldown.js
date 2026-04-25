// Database drilldown sub-modules under /api/v1/saturation/database/{group}/*.
// Endpoints (all GET, query: startTime+endTime):
//   /system/{latency,ops,top-collections-by-latency,top-collections-by-volume,
//            errors,namespaces}
//   /connections/{count,utilization,limits,pending,timeout-rate,
//                 wait-time,create-time,use-time}
//   /collection/{latency,ops,errors,query-texts,read-vs-write}
//   /errors/{by-system,by-operation,by-error-type,by-collection,
//            by-status,ratio}
//   /latency/{by-system,by-operation,by-collection,by-namespace,
//             by-server,heatmap}
//   /slow-queries/{patterns,collections,rate,p99-by-text}
//   /ops/{by-system,by-operation,by-collection,read-vs-write,by-namespace}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const GROUPS = {
  system:        ['latency', 'ops', 'top-collections-by-latency', 'top-collections-by-volume', 'errors', 'namespaces'],
  connections:   ['count', 'utilization', 'limits', 'pending', 'timeout-rate', 'wait-time', 'create-time', 'use-time'],
  collection:    ['latency', 'ops', 'errors', 'query-texts', 'read-vs-write'],
  errors:        ['by-system', 'by-operation', 'by-error-type', 'by-collection', 'by-status', 'ratio'],
  latency:       ['by-system', 'by-operation', 'by-collection', 'by-namespace', 'by-server', 'heatmap'],
  'slow-queries':['patterns', 'collections', 'rate', 'p99-by-text'],
  ops:           ['by-system', 'by-operation', 'by-collection', 'read-vs-write', 'by-namespace'],
};

export function saturationDatastoresDrilldown(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  for (const group of Object.keys(GROUPS)) {
    for (const leaf of GROUPS[group]) {
      const path = `/api/v1/saturation/database/${group}/${leaf}`;
      client.get(path, q, { module: MOD, endpoint: `GET /saturation/database/${group}/${leaf}` });
    }
  }
}
