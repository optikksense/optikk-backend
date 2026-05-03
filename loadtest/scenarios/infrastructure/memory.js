// Infrastructure / memory submodule.
// Routes (all GET):
//   /api/v1/infrastructure/memory/{usage,usage-percentage,swap,avg}    — time only
//   /api/v1/infrastructure/memory/{by-service,by-instance}             — require serviceName

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const TIMESERIES = ['usage', 'usage-percentage', 'swap', 'avg'];

export function infraMemory(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  for (const leaf of TIMESERIES) {
    client.get(`/api/v1/infrastructure/memory/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/memory/${leaf}` });
  }

  const svc = randomPick(services);
  client.get('/api/v1/infrastructure/memory/by-service',
    { ...q, serviceName: svc },
    { module: MOD, endpoint: 'GET /infrastructure/memory/by-service' });
  client.get('/api/v1/infrastructure/memory/by-instance',
    { ...q, serviceName: svc, host: '', pod: '', container: '' },
    { module: MOD, endpoint: 'GET /infrastructure/memory/by-instance' });
}
