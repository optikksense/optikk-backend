// Shared helper: cheap explorer query that returns a single trace+span pair
// for fan-out scenarios that need real ids (detail, paths_and_shape).
// Stays inside scenarios/traces/ to keep the dependency tree obvious.

import { tracesQueryBody } from '../../lib/payloads.js';
import { windowFor } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

export function pickTraceAndSpan(client, mod) {
  const w = windowFor(cfg.lookback);
  const list = client.post('/api/v1/traces/query',
    tracesQueryBody({ ...w, include: ['summary'], limit: 5 }),
    { module: mod, endpoint: 'POST /traces/query (discover)' },
  );
  const results = list && list.data && (list.data.results || list.data.traces || list.data.items);
  if (!Array.isArray(results) || results.length === 0) return { traceId: null, spanId: null };
  const first = results[0];
  return {
    traceId: first.traceId || first.trace_id || first.id || null,
    spanId:  first.rootSpanId || first.spanId || first.root_span_id || first.span_id || null,
  };
}
