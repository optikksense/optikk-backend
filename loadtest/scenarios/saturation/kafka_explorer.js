// Kafka explorer: cluster summary, topic + group inventory, drilldowns.
// Endpoints (all GET, query varies):
//   /api/v1/saturation/kafka/{summary,topics,groups}
//   /api/v1/saturation/kafka/topic/{overview,groups,partitions} ?topic=
//   /api/v1/saturation/kafka/group/{overview,topics,partitions} ?group=

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, kafkaTopics, kafkaGroups } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const TOPIC_PATHS = ['overview', 'groups', 'partitions'];
const GROUP_PATHS = ['overview', 'topics', 'partitions'];

function pickFromList(res, fallback, key) {
  const data = res && res.data;
  const arr = Array.isArray(data) ? data : (data && (data[key] || data.results || data.items)) || [];
  if (arr && arr.length > 0) return arr[0].name || arr[0][key.slice(0, -1)] || arr[0].id || fallback;
  return fallback;
}

export function saturationKafkaExplorer(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  client.get('/api/v1/saturation/kafka/summary', q,
    { module: MOD, endpoint: 'GET /saturation/kafka/summary' });
  const topics = client.get('/api/v1/saturation/kafka/topics', q,
    { module: MOD, endpoint: 'GET /saturation/kafka/topics' });
  const groups = client.get('/api/v1/saturation/kafka/groups', q,
    { module: MOD, endpoint: 'GET /saturation/kafka/groups' });

  const topic = pickFromList(topics, randomPick(kafkaTopics), 'topics');
  const group = pickFromList(groups, randomPick(kafkaGroups), 'groups');

  for (const seg of TOPIC_PATHS) {
    client.get(`/api/v1/saturation/kafka/topic/${seg}`, { ...q, topic },
      { module: MOD, endpoint: `GET /saturation/kafka/topic/${seg}` });
  }
  for (const seg of GROUP_PATHS) {
    client.get(`/api/v1/saturation/kafka/group/${seg}`, { ...q, group },
      { module: MOD, endpoint: `GET /saturation/kafka/group/${seg}` });
  }
}
