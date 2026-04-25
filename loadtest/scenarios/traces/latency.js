// Trace latency analytics: histogram + heatmap.
// Endpoints:
//   GET /api/v1/latency/histogram
//   GET /api/v1/latency/heatmap

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function traceLatency(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  client.get('/api/v1/latency/histogram', { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /latency/histogram' });
  client.get('/api/v1/latency/heatmap', { startTime: w.startTime, endTime: w.endTime },
    { module: MOD, endpoint: 'GET /latency/heatmap' });
}
