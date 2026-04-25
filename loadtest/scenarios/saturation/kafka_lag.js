// Kafka consumer lag + rebalance signals.
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/saturation/kafka/{consumer-lag-by-group,lag-by-group,
//          lag-per-partition,assigned-partitions,rebalance-signals,
//          e2e-latency}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const PATHS = [
  'consumer-lag-by-group', 'lag-by-group', 'lag-per-partition',
  'assigned-partitions', 'rebalance-signals', 'e2e-latency',
];

export function saturationKafkaLag(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/saturation/kafka/${seg}`,
      { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /saturation/kafka/${seg}` });
  }
}
