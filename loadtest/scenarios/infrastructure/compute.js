// Infrastructure compute metrics: cpu/, memory/, jvm/.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/cpu/{time,usage-percentage,load-average,
//                               process-count,avg,by-service,by-instance}
//   /api/v1/infrastructure/memory/{usage,usage-percentage,swap,avg,
//                                  by-service,by-instance}
//   /api/v1/infrastructure/jvm/{memory,gc-duration,gc-collections,threads,
//                               classes,cpu,buffers}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const CPU = ['time', 'usage-percentage', 'load-average', 'process-count', 'avg', 'by-service', 'by-instance'];
const MEM = ['usage', 'usage-percentage', 'swap', 'avg', 'by-service', 'by-instance'];
const JVM = ['memory', 'gc-duration', 'gc-collections', 'threads', 'classes', 'cpu', 'buffers'];

function hit(client, q, base, leaves) {
  for (const seg of leaves) {
    client.get(`/api/v1/infrastructure/${base}/${seg}`, q,
      { module: MOD, endpoint: `GET /infrastructure/${base}/${seg}` });
  }
}

export function infraCompute(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  hit(client, q, 'cpu', CPU);
  hit(client, q, 'memory', MEM);
  hit(client, q, 'jvm', JVM);
}
