// Infrastructure I/O metrics: disk/, network/, connpool/.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/disk/{io,operations,io-time,filesystem-usage,
//                                filesystem-utilization,avg,by-service,by-instance}
//   /api/v1/infrastructure/network/{io,packets,errors,dropped,connections,
//                                   avg,by-service,by-instance}
//   /api/v1/infrastructure/connpool/{avg,by-service,by-instance}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const DISK = ['io', 'operations', 'io-time', 'filesystem-usage', 'filesystem-utilization', 'avg', 'by-service', 'by-instance'];
const NET  = ['io', 'packets', 'errors', 'dropped', 'connections', 'avg', 'by-service', 'by-instance'];
const POOL = ['avg', 'by-service', 'by-instance'];

function hit(client, q, base, leaves) {
  for (const seg of leaves) {
    client.get(`/api/v1/infrastructure/${base}/${seg}`, q,
      { module: MOD, endpoint: `GET /infrastructure/${base}/${seg}` });
  }
}

export function infraIO(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  hit(client, q, 'disk', DISK);
  hit(client, q, 'network', NET);
  hit(client, q, 'connpool', POOL);
}
