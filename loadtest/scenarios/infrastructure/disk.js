// Infrastructure / disk submodule.
// Routes (all GET):
//   /api/v1/infrastructure/disk/{io,operations,io-time,filesystem-usage,
//                                filesystem-utilization,avg}            — time only
//   /api/v1/infrastructure/disk/{by-service,by-instance}                 — require serviceName

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const TIMESERIES = ['io', 'operations', 'io-time', 'filesystem-usage', 'filesystem-utilization', 'avg'];

export function infraDisk(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  for (const leaf of TIMESERIES) {
    client.get(`/api/v1/infrastructure/disk/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/disk/${leaf}` });
  }

  const svc = randomPick(services);
  client.get('/api/v1/infrastructure/disk/by-service',
    { ...q, serviceName: svc },
    { module: MOD, endpoint: 'GET /infrastructure/disk/by-service' });
  client.get('/api/v1/infrastructure/disk/by-instance',
    { ...q, serviceName: svc, host: '', pod: '', container: '' },
    { module: MOD, endpoint: 'GET /infrastructure/disk/by-instance' });
}
