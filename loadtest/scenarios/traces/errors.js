// Trace-side error endpoints (under /api/v1/errors and /api/v1/services).
// Endpoints:
//   GET /api/v1/errors/groups
//   GET /api/v1/errors/timeseries
//   GET /api/v1/services/:serviceName/errors
//   GET /api/v1/services/:serviceName/errors/timeseries

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function traceErrors(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const svc = randomPick(services);

  client.get('/api/v1/errors/groups', { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /errors/groups' });
  client.get('/api/v1/errors/timeseries', { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /errors/timeseries' });
  client.get(`/api/v1/services/${encodeURIComponent(svc)}/errors`,
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /services/:serviceName/errors' });
  client.get(`/api/v1/services/${encodeURIComponent(svc)}/errors/timeseries`,
    { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /services/:serviceName/errors/timeseries' });
}
