// Metrics meta: list metric names + per-metric tag listing.
// Endpoints:
//   GET /api/v1/metrics/names
//   GET /api/v1/metrics/:metricName/tags

import { buildClient } from '../../lib/client.js';
import { randomPick, metricNames } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'metrics';

export function metricsMeta(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });

  const names = client.get('/api/v1/metrics/names', null,
    { module: MOD, endpoint: 'GET /metrics/names' });

  const known = (names && names.data && Array.isArray(names.data) && names.data.length > 0)
    ? names.data
    : metricNames;
  const metric = randomPick(known);
  if (!metric) return;

  client.get(`/api/v1/metrics/${encodeURIComponent(metric)}/tags`, null,
    { module: MOD, endpoint: 'GET /metrics/:metricName/tags' });
}
