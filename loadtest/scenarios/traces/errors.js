// Span error endpoints, served by services/errors.
// Endpoints:
//   GET /api/v1/errors/groups[?serviceName=]
//   GET /api/v1/errors/error-volume[?serviceName=]

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function traceErrors(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const svc = randomPick(services);
  const q = { startTime: w.startTime, endTime: w.endTime };
  const svcQ = { ...q, serviceName: svc };

  client.get('/api/v1/errors/groups', q,
    { module: MOD, endpoint: 'GET /errors/groups' });
  client.get('/api/v1/errors/error-volume', q,
    { module: MOD, endpoint: 'GET /errors/error-volume' });
  client.get('/api/v1/errors/groups', svcQ,
    { module: MOD, endpoint: 'GET /errors/groups?serviceName' });
  client.get('/api/v1/errors/error-volume', svcQ,
    { module: MOD, endpoint: 'GET /errors/error-volume?serviceName' });
}
