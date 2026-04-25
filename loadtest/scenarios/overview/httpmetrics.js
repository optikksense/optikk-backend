// HTTP server/client metrics + per-route + external-host drilldowns.
// Endpoints:
//   GET /api/v1/http/{request-rate,request-duration,active-requests,
//                     request-body-size,response-body-size,client-duration,
//                     dns-duration,tls-duration,status-distribution,
//                     error-timeseries}
//   GET /api/v1/http/routes/{top-by-volume,top-by-latency,error-rate,
//                            error-timeseries}
//   GET /api/v1/http/external/{top-hosts,host-latency,error-rate}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { cfg } from '../../lib/config.js';

const MOD = 'overview';
const HTTP_PATHS = [
  'request-rate', 'request-duration', 'active-requests',
  'request-body-size', 'response-body-size', 'client-duration',
  'dns-duration', 'tls-duration', 'status-distribution', 'error-timeseries',
];
const ROUTE_PATHS = ['top-by-volume', 'top-by-latency', 'error-rate', 'error-timeseries'];
const EXTERNAL_PATHS = ['top-hosts', 'host-latency', 'error-rate'];

export function overviewHttpMetrics(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  for (const seg of HTTP_PATHS) {
    client.get(`/api/v1/http/${seg}`, q,
      { module: MOD, endpoint: `GET /http/${seg}` });
  }
  for (const seg of ROUTE_PATHS) {
    client.get(`/api/v1/http/routes/${seg}`, q,
      { module: MOD, endpoint: `GET /http/routes/${seg}` });
  }
  for (const seg of EXTERNAL_PATHS) {
    client.get(`/api/v1/http/external/${seg}`, q,
      { module: MOD, endpoint: `GET /http/external/${seg}` });
  }
}
