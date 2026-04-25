// Trace detail (extras): span events, span logs, trace logs, related traces.
// Endpoints:
//   GET /api/v1/traces/:traceId/span-events
//   GET /api/v1/traces/:traceId/spans/:spanId/logs
//   GET /api/v1/traces/:traceId/logs
//   GET /api/v1/traces/:traceId/related

import { buildClient } from '../../lib/client.js';
import { pickTraceAndSpan } from './_discover.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function traceDetailExtras(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const { traceId, spanId } = pickTraceAndSpan(client, MOD);
  if (!traceId) return;

  client.get(`/api/v1/traces/${traceId}/span-events`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/span-events' });
  client.get(`/api/v1/traces/${traceId}/logs`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/logs' });
  client.get(`/api/v1/traces/${traceId}/related`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/related' });

  if (spanId) {
    client.get(`/api/v1/traces/${traceId}/spans/${spanId}/logs`, null,
      { module: MOD, endpoint: 'GET /traces/:traceId/spans/:spanId/logs' });
  }
}
