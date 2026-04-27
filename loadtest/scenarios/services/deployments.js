// Deployment metadata + traffic + impact analysis.
// Endpoints (all GET):
//   /api/v1/deployments/latest-by-service
//   /api/v1/deployments/list
//   /api/v1/deployments/compare
//   /api/v1/deployments/timeline
//   /api/v1/deployments/impact
//   /api/v1/deployments/active-version

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'services';

export function servicesDeployments(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };
  const svc = randomPick(services);
  const sq = { ...q, serviceName: svc };

  const latest = client.get('/api/v1/deployments/latest-by-service', q,
    { module: MOD, endpoint: 'GET /deployments/latest-by-service' });
  const list = client.get('/api/v1/deployments/list', sq,
    { module: MOD, endpoint: 'GET /deployments/list' });
  client.get('/api/v1/deployments/timeline', sq,
    { module: MOD, endpoint: 'GET /deployments/timeline' });
  client.get('/api/v1/deployments/impact', sq,
    { module: MOD, endpoint: 'GET /deployments/impact' });
  client.get('/api/v1/deployments/active-version', sq,
    { module: MOD, endpoint: 'GET /deployments/active-version' });

  let sample = null;
  if (list && list.data && Array.isArray(list.data.deployments) && list.data.deployments.length > 0) {
    sample = list.data.deployments[0];
  } else if (latest && latest.data && Array.isArray(latest.data)) {
    sample = latest.data.find((row) => row && row.service_name === svc);
  }
  if (sample && sample.version && sample.first_seen) {
    client.get('/api/v1/deployments/compare', {
      ...sq,
      version: sample.version,
      environment: sample.environment || '',
      deployedAt: new Date(sample.first_seen).getTime(),
    }, { module: MOD, endpoint: 'GET /deployments/compare' });
  }
}
