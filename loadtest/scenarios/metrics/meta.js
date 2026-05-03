// Metrics meta: list metric names + per-metric tag listing.
// Endpoints:
//   GET /api/v1/metrics/names           — requires startTime+endTime
//   GET /api/v1/metrics/:metricName/tags — requires startTime+endTime

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, metricNames } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'metrics';

export function metricsMeta(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  const names = client.get('/api/v1/metrics/names', q,
    { module: MOD, endpoint: 'GET /metrics/names' });

  const known = (names && names.data && names.data.metrics && Array.isArray(names.data.metrics) && names.data.metrics.length > 0)
    ? names.data.metrics.map(m => m.name)
    : metricNames;
  const metric = randomPick(known);
  if (!metric) return;

  client.get(`/api/v1/metrics/${encodeURIComponent(metric)}/tags`, q,
    { module: MOD, endpoint: 'GET /metrics/:metricName/tags' });
}
