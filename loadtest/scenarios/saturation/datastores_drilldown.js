// Composite scenario: walks every saturation/database submodule once per
// iteration. Per-submodule scenarios live under scenarios/saturation/database/
// and can be exercised standalone via entrypoints/granular.js.

import { dbVolume }       from './database/volume.js';
import { dbErrors }       from './database/errors.js';
import { dbLatency }      from './database/latency.js';
import { dbCollection }   from './database/collection.js';
import { dbSystem }       from './database/system.js';
import { dbSystems }      from './database/systems.js';
import { dbSummary }      from './database/summary.js';
import { dbSlowQueries }  from './database/slowqueries.js';
import { dbConnections } from './database/connections.js';

export function saturationDatastoresDrilldown(ctx) {
  dbSummary(ctx);
  dbSystems(ctx);
  dbVolume(ctx);
  dbErrors(ctx);
  dbLatency(ctx);
  dbSlowQueries(ctx);
  dbConnections(ctx);
  dbSystem(ctx);
  dbCollection(ctx);
}
