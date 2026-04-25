// APM endpoints — RPC + messaging + process metrics.
// Endpoints:
//   GET /api/v1/apm/{rpc-duration,rpc-request-rate,messaging-publish-duration,
//                    process-cpu,process-memory,open-fds,uptime}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const PATHS = [
  'rpc-duration', 'rpc-request-rate', 'messaging-publish-duration',
  'process-cpu', 'process-memory', 'open-fds', 'uptime',
];

export function overviewApm(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/apm/${seg}`, { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /apm/${seg}` });
  }
}
