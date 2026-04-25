// Infrastructure nodes: list + summary + per-host services drilldown.
// Endpoints:
//   GET /api/v1/infrastructure/nodes
//   GET /api/v1/infrastructure/nodes/summary
//   GET /api/v1/infrastructure/nodes/:host/services

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';

export function infraNodes(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  const list = client.get('/api/v1/infrastructure/nodes', q,
    { module: MOD, endpoint: 'GET /infrastructure/nodes' });
  client.get('/api/v1/infrastructure/nodes/summary', q,
    { module: MOD, endpoint: 'GET /infrastructure/nodes/summary' });

  const arr = list && list.data && (Array.isArray(list.data) ? list.data : (list.data.nodes || []));
  const host = arr && arr.length > 0 && (arr[0].host || arr[0].hostname || arr[0].name);
  if (host) {
    client.get(`/api/v1/infrastructure/nodes/${encodeURIComponent(host)}/services`, q,
      { module: MOD, endpoint: 'GET /infrastructure/nodes/:host/services' });
  }
}
