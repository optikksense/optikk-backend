// Kubernetes workload metrics.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/infrastructure/kubernetes/{container-cpu,cpu-throttling,
//          container-memory,oom-kills,pod-restarts,node-allocatable,
//          pod-phases,replica-status,volume-usage}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'infrastructure';
const PATHS = [
  'container-cpu', 'cpu-throttling', 'container-memory', 'oom-kills',
  'pod-restarts', 'node-allocatable', 'pod-phases', 'replica-status', 'volume-usage',
];

export function infraKubernetes(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/infrastructure/kubernetes/${seg}`,
      { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /infrastructure/kubernetes/${seg}` });
  }
}
