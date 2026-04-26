// Infrastructure compute metrics: cpu/, memory/, jvm/.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/cpu/{time,usage-percentage,load-average,
//                               process-count,avg,by-service,by-instance}
//   /api/v1/infrastructure/memory/{usage,usage-percentage,swap,avg,
//                                  by-service,by-instance}
//   /api/v1/infrastructure/jvm/{memory,gc-duration,gc-collections,threads,
//                               classes,cpu,buffers}
//
// by-service and by-instance under memory/ and disk/ require `serviceName`.
// cpu/ and jvm/ by-service/by-instance do NOT require it.

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';

// cpu by-service/by-instance take only time params (they enumerate all services)
const CPU = ['time', 'usage-percentage', 'load-average', 'process-count', 'avg', 'by-service', 'by-instance'];

// memory timeseries endpoints (no extra params)
const MEM_TIMESERIES = ['usage', 'usage-percentage', 'swap', 'avg'];

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
  hit(client, q, 'memory', MEM_TIMESERIES);
  hit(client, q, 'jvm', JVM);

  // memory by-service and by-instance require serviceName
  const svc = randomPick(services);
  client.get('/api/v1/infrastructure/memory/by-service',
    { ...q, serviceName: svc },
    { module: MOD, endpoint: 'GET /infrastructure/memory/by-service' });
  client.get('/api/v1/infrastructure/memory/by-instance',
    { ...q, serviceName: svc, host: '', pod: '', container: '' },
    { module: MOD, endpoint: 'GET /infrastructure/memory/by-instance' });
}
