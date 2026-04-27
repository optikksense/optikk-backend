// Overview errors: error volume + groups + drilldown into a single group.
// Endpoints:
//   GET /api/v1/overview/errors/{service-error-rate,error-volume,
//                                latency-during-error-windows,groups}
//   GET /api/v1/errors/groups/:groupId
//   GET /api/v1/errors/groups/:groupId/traces
//   GET /api/v1/errors/groups/:groupId/timeseries
//   GET /api/v1/errors/fingerprints
//   GET /api/v1/errors/fingerprints/trend

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const TOP = [
  '/api/v1/overview/errors/service-error-rate',
  '/api/v1/overview/errors/error-volume',
  '/api/v1/overview/errors/latency-during-error-windows',
  '/api/v1/overview/errors/groups',
];

export function overviewErrors(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  let sampleGroupId = null;
  for (const p of TOP) {
    const res = client.get(p, q, { module: MOD, endpoint: `GET ${p.replace('/api/v1', '')}` });
    if (!sampleGroupId && p.endsWith('/groups') && res && res.data) {
      const arr = Array.isArray(res.data) ? res.data : (res.data.groups || res.data.results || []);
      if (arr && arr.length > 0) sampleGroupId = arr[0].id || arr[0].groupId || arr[0].fingerprint;
    }
  }

  const fps = client.get('/api/v1/errors/fingerprints', { ...q, limit: 25 },
    { module: MOD, endpoint: 'GET /errors/fingerprints' });

  const fpArr = fps && fps.data && (Array.isArray(fps.data) ? fps.data : (fps.data.results || fps.data.fingerprints || []));
  const fp = fpArr && fpArr.length > 0 ? fpArr[0] : null;
  if (fp && fp.serviceName && fp.operationName) {
    client.get('/api/v1/errors/fingerprints/trend', {
      ...q,
      serviceName: fp.serviceName,
      operationName: fp.operationName,
      exceptionType: fp.exceptionType,
      statusMessage: fp.statusMessage,
    }, { module: MOD, endpoint: 'GET /errors/fingerprints/trend' });
  }

  if (sampleGroupId) {
    const enc = encodeURIComponent(sampleGroupId);
    client.get(`/api/v1/errors/groups/${enc}`, q,
      { module: MOD, endpoint: 'GET /errors/groups/:groupId' });
    client.get(`/api/v1/errors/groups/${enc}/traces`, { ...q, limit: 25 },
      { module: MOD, endpoint: 'GET /errors/groups/:groupId/traces' });
    client.get(`/api/v1/errors/groups/${enc}/timeseries`, q,
      { module: MOD, endpoint: 'GET /errors/groups/:groupId/timeseries' });
  }
}
