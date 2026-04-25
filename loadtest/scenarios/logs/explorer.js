// Logs Explorer: search.
// Endpoints:
//   POST /api/v1/logs/query

import { buildClient } from '../../lib/client.js';
import { logsQueryBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services, severities } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'logs';

export function logsExplorer(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  client.post('/api/v1/logs/query',
    logsQueryBody({
      ...w,
      filters: [
        { field: 'service',       op: 'eq', value: randomPick(services) },
        { field: 'severity_text', op: 'eq', value: randomPick(severities) },
      ],
      include: ['summary', 'facets', 'trend'],
      limit: 100,
    }),
    { module: MOD, endpoint: 'POST /logs/query' },
  );
}
