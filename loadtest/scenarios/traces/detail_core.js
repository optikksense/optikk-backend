// Trace detail (core): bundle, spans list, span tree, span attributes.
// Discovers a trace+span via the explorer, then fans out across the four
// detail sub-routes that anchor the trace UI.
// Endpoints:
//   GET /api/v1/traces/:traceId/bundle
//   GET /api/v1/traces/:traceId/spans
//   GET /api/v1/spans/:spanId/tree
//   GET /api/v1/traces/:traceId/spans/:spanId/attributes

import { buildClient } from '../../lib/client.js';
import { pickTraceAndSpan } from './_discover.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function traceDetailCore(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const { traceId, spanId } = pickTraceAndSpan(client, MOD);
  if (!traceId) return;

  client.get(`/api/v1/traces/${traceId}/bundle`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/bundle' });
  client.get(`/api/v1/traces/${traceId}/spans`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/spans' });

  if (spanId) {
    client.get(`/api/v1/spans/${spanId}/tree`, null,
      { module: MOD, endpoint: 'GET /spans/:spanId/tree' });
    client.get(`/api/v1/traces/${traceId}/spans/${spanId}/attributes`, null,
      { module: MOD, endpoint: 'GET /traces/:traceId/spans/:spanId/attributes' });
  }
}
