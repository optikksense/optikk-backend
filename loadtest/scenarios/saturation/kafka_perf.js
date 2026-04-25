// Kafka throughput + latency timeseries (publish/consume/process).
// Endpoints (all GET, query: startTime+endTime):
//   /api/v1/saturation/kafka/{summary-stats,produce-rate-by-topic,
//          publish-latency-by-topic,consume-rate-by-topic,
//          receive-latency-by-topic,consume-rate-by-group,
//          process-rate-by-group,process-latency-by-group}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const PATHS = [
  'summary-stats', 'produce-rate-by-topic', 'publish-latency-by-topic',
  'consume-rate-by-topic', 'receive-latency-by-topic',
  'consume-rate-by-group', 'process-rate-by-group', 'process-latency-by-group',
];

export function saturationKafkaPerf(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  for (const seg of PATHS) {
    client.get(`/api/v1/saturation/kafka/${seg}`,
      { startTime: w.startTime, endTime: w.endTime },
      { module: MOD, endpoint: `GET /saturation/kafka/${seg}` });
  }
}
