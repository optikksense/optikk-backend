// k6 smoke entrypoint: a single VU walks through one endpoint per module
// once. Used by CI to confirm bootstrap, auth, routing, and envelope all
// agree end-to-end. Ignores cfg.rps/duration; runs as fast as the server
// allows for `iterations` total iterations.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { buildClient } from '../lib/client.js';
import { adaptiveWindow } from '../lib/timewindows.js';
import { tracesQueryBody, logsQueryBody, metricsQueryBody } from '../lib/payloads.js';
import { metricNames, services, randomPick } from '../lib/fixtures.js';
import { cfg } from '../lib/config.js';

export const options = {
  scenarios: {
    smoke: {
      executor: 'per-vu-iterations',
      exec: 'smoke',
      vus: 1, iterations: 1, maxDuration: '60s',
    },
  },
};

export function smoke(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  client.post('/api/v1/traces/query', tracesQueryBody({ ...w, limit: 5 }),
    { module: 'smoke', endpoint: 'traces' });
  client.post('/api/v1/logs/query', logsQueryBody({ ...w, limit: 5 }),
    { module: 'smoke', endpoint: 'logs' });
  client.post('/api/v1/metrics/explorer/query',
    metricsQueryBody({ metricNames: [randomPick(metricNames)], aggregation: 'avg', ...w, step: '60s' }),
    { module: 'smoke', endpoint: 'metrics' });
  client.get('/api/v1/overview/summary', q, { module: 'smoke', endpoint: 'overview' });
  client.get('/api/v1/infrastructure/nodes', q, { module: 'smoke', endpoint: 'infrastructure' });
  client.get('/api/v1/saturation/datastores/summary', q, { module: 'smoke', endpoint: 'saturation_explorer' });
  client.get('/api/v1/saturation/database/summary',   q, { module: 'smoke', endpoint: 'saturation_db_summary' });
  client.get('/api/v1/saturation/database/systems',   q, { module: 'smoke', endpoint: 'saturation_db_systems' });
  client.get('/api/v1/services/topology', q, { module: 'smoke', endpoint: 'services' });
}

export { setup, teardown, handleSummary };
