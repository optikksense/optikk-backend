// Infrastructure I/O metrics: disk/, network/, connpool/.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/disk/{io,operations,io-time,filesystem-usage,
//                                filesystem-utilization,avg,by-service,by-instance}
//   /api/v1/infrastructure/network/{io,packets,errors,dropped,connections,
//                                   avg,by-service,by-instance}
//   /api/v1/infrastructure/connpool/{avg,by-service,by-instance}
//
// disk/ and network/ by-service & by-instance require `serviceName`.
// connpool/ by-service & by-instance do NOT require it.

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';

// Timeseries endpoints that need only time params
const DISK_TS = ['io', 'operations', 'io-time', 'filesystem-usage', 'filesystem-utilization', 'avg'];
const NET_TS  = ['io', 'packets', 'errors', 'dropped', 'connections', 'avg'];

// connpool by-service/by-instance do NOT require serviceName (they enumerate all)
const POOL = ['avg', 'by-service', 'by-instance'];

function hit(client, q, base, leaves) {
  for (const seg of leaves) {
    client.get(`/api/v1/infrastructure/${base}/${seg}`, q,
      { module: MOD, endpoint: `GET /infrastructure/${base}/${seg}` });
  }
}

// Hit drilldown endpoints that require serviceName
function hitDrilldown(client, q, base) {
  const svc = randomPick(services);
  client.get(`/api/v1/infrastructure/${base}/by-service`,
    { ...q, serviceName: svc },
    { module: MOD, endpoint: `GET /infrastructure/${base}/by-service` });
  client.get(`/api/v1/infrastructure/${base}/by-instance`,
    { ...q, serviceName: svc, host: '', pod: '', container: '' },
    { module: MOD, endpoint: `GET /infrastructure/${base}/by-instance` });
}

export function infraIO(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  hit(client, q, 'disk', DISK_TS);
  hitDrilldown(client, q, 'disk');

  hit(client, q, 'network', NET_TS);
  hitDrilldown(client, q, 'network');

  hit(client, q, 'connpool', POOL);
}
