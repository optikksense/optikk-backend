// Span Query + Trace Suggest (autocomplete) scenarios.
// Endpoints exercised:
//   POST /api/v1/spans/query
//   POST /api/v1/traces/suggest

import { buildClient } from '../../lib/client.js';
import { spansQueryBody, traceSuggestBody } from '../../lib/payloads.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, services, attributeKeys, spanKinds } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

const SUGGEST_FIELDS = ['service', 'operation', '@http.target', '@http.method'];
const SUGGEST_PREFIXES = ['', 'g', 'p', 'a', 'd'];

export function spansQuery(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  client.post('/api/v1/spans/query',
    spansQueryBody({
      ...w,
      filters: [
        { field: 'service',   op: 'eq',       value: randomPick(services) },
        { field: 'span_kind', op: 'eq',       value: randomPick(spanKinds) },
        { field: randomPick(attributeKeys), op: 'contains', value: '' },
      ],
      limit: 50,
    }),
    { module: MOD, endpoint: 'POST /spans/query' },
  );
}

export function tracesSuggest(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);

  client.post('/api/v1/traces/suggest',
    traceSuggestBody({
      ...w,
      field:  randomPick(SUGGEST_FIELDS),
      prefix: randomPick(SUGGEST_PREFIXES),
      limit:  20,
    }),
    { module: MOD, endpoint: 'POST /traces/suggest' },
  );
}
