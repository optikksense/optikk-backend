// Datastore explorer: top-level summary + system list + per-system overview.
// Endpoints (all GET, query: startTime+endTime[, system]):
//   /api/v1/saturation/datastores/summary
//   /api/v1/saturation/datastores/systems
//   /api/v1/saturation/datastores/system/{overview,servers,namespaces,
//          operations,errors,connections,slow-queries}

import { buildClient } from '../../lib/client.js';
import { adaptiveWindow } from '../../lib/timewindows.js';
import { randomPick, datastoreSystems } from '../../lib/fixtures.js';
import { cfg } from '../../lib/config.js';

const MOD = 'saturation';
const SYSTEM_PATHS = ['overview', 'servers', 'namespaces', 'operations', 'errors', 'connections', 'slow-queries'];

export function saturationDatastoresExplorer(ctx) {
  const client = buildClient({ ...ctx, baseUrl: cfg.baseUrl });
  const w = adaptiveWindow(cfg.lookback);
  const q = { startTime: w.startTime, endTime: w.endTime };

  client.get('/api/v1/saturation/datastores/summary', q,
    { module: MOD, endpoint: 'GET /saturation/datastores/summary' });
  const systems = client.get('/api/v1/saturation/datastores/systems', q,
    { module: MOD, endpoint: 'GET /saturation/datastores/systems' });

  const arr = systems && systems.data && (Array.isArray(systems.data) ? systems.data : (systems.data.systems || []));
  const sysName = (arr && arr.length > 0 && (arr[0].system || arr[0].name || arr[0].id)) || randomPick(datastoreSystems);

  const sq = { ...q, system: sysName };
  for (const seg of SYSTEM_PATHS) {
    client.get(`/api/v1/saturation/datastores/system/${seg}`, sq,
      { module: MOD, endpoint: `GET /saturation/datastores/system/${seg}` });
  }
}
