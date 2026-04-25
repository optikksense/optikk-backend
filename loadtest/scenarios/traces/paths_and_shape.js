// Trace paths + shape: critical-path, error-path, service-map, errors,
// span-kind-breakdown, flamegraph.
// Endpoints:
//   GET /api/v1/traces/:traceId/critical-path
//   GET /api/v1/traces/:traceId/error-path
//   GET /api/v1/traces/:traceId/service-map
//   GET /api/v1/traces/:traceId/errors
//   GET /api/v1/traces/:traceId/span-kind-breakdown
//   GET /api/v1/traces/:traceId/flamegraph

import { buildClient } from '../../lib/client.js';
import { pickTraceAndSpan } from './_discover.js';
import { cfg } from '../../lib/config.js';

const MOD = 'traces';

export function tracePathsAndShape(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const { traceId } = pickTraceAndSpan(client, MOD);
  if (!traceId) return;

  client.get(`/api/v1/traces/${traceId}/critical-path`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/critical-path' });
  client.get(`/api/v1/traces/${traceId}/error-path`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/error-path' });
  client.get(`/api/v1/traces/${traceId}/service-map`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/service-map' });
  client.get(`/api/v1/traces/${traceId}/errors`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/errors' });
  client.get(`/api/v1/traces/${traceId}/span-kind-breakdown`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/span-kind-breakdown' });
  client.get(`/api/v1/traces/${traceId}/flamegraph`, null,
    { module: MOD, endpoint: 'GET /traces/:traceId/flamegraph' });
}
