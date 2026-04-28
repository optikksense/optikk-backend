// Infrastructure / network submodule.
// Routes (all GET):
//   /api/v1/infrastructure/network/{io,packets,errors,dropped,connections,avg} — time only
//   /api/v1/infrastructure/network/{by-service,by-instance}                    — require serviceName

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const TIMESERIES = ['io', 'packets', 'errors', 'dropped', 'connections', 'avg'];

export function infraNetwork(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  for (const leaf of TIMESERIES) {
    client.get(`/api/v1/infrastructure/network/${leaf}`, q,
      { module: MOD, endpoint: `GET /infrastructure/network/${leaf}` });
  }

  const svc = randomPick(services);
  client.get('/api/v1/infrastructure/network/by-service',
    { ...q, serviceName: svc },
    { module: MOD, endpoint: 'GET /infrastructure/network/by-service' });
  client.get('/api/v1/infrastructure/network/by-instance',
    { ...q, serviceName: svc, host: '', pod: '', container: '' },
    { module: MOD, endpoint: 'GET /infrastructure/network/by-instance' });
}
