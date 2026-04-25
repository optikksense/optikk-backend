// Kafka health: error counters + broker connections.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/saturation/kafka/{publish-errors,consume-errors,process-errors,
//          client-op-errors,broker-connections,client-op-duration}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const PATHS = [
  'publish-errors', 'consume-errors', 'process-errors',
  'client-op-errors', 'broker-connections', 'client-op-duration',
];

export function saturationKafkaHealth(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/saturation/kafka/${seg}`,
      { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /saturation/kafka/${seg}` });
  }
}
