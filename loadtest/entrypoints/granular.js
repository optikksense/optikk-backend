// k6 entrypoint: every submodule as its own scenario. Use this to measure
// per-submodule throughput/latency in isolation rather than per top-level
// module. Each submodule runs at cfg.rps independently — total RPS is
// (number-of-submodules × cfg.rps), so dial cfg.rps DOWN compared to a
// composite run.
//
// Run a single submodule:
//   k6 run -e RPS=10 --scenario=db_connections loadtest/entrypoints/granular.js
//
// Run all submodules at once:
//   k6 run -e RPS=2 -e DURATION=2m loadtest/entrypoints/granular.js

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { dbVolume }      from '../scenarios/saturation/database/volume.js';
import { dbErrors }      from '../scenarios/saturation/database/errors.js';
import { dbLatency }     from '../scenarios/saturation/database/latency.js';
import { dbCollection }  from '../scenarios/saturation/database/collection.js';
import { dbSystem }      from '../scenarios/saturation/database/system.js';
import { dbSystems }     from '../scenarios/saturation/database/systems.js';
import { dbSummary }     from '../scenarios/saturation/database/summary.js';
import { dbSlowQueries } from '../scenarios/saturation/database/slowqueries.js';
import { dbConnections } from '../scenarios/saturation/database/connections.js';

import { infraCPU }      from '../scenarios/infrastructure/cpu.js';
import { infraMemory }   from '../scenarios/infrastructure/memory.js';
import { infraJVM }      from '../scenarios/infrastructure/jvm.js';
import { infraDisk }     from '../scenarios/infrastructure/disk.js';
import { infraNetwork }  from '../scenarios/infrastructure/network.js';
import { infraConnPool } from '../scenarios/infrastructure/connpool.js';
import { infraNodes }    from '../scenarios/infrastructure/nodes.js';
import { infraFleet }    from '../scenarios/infrastructure/fleet.js';
import { infraResourceUtil } from '../scenarios/infrastructure/resource_util.js';
import { infraKubernetes }   from '../scenarios/infrastructure/kubernetes.js';

function block(module, name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module },
  };
}

export const options = {
  scenarios: {
    db_volume:        block('saturation', 'dbVolume'),
    db_errors:        block('saturation', 'dbErrors'),
    db_latency:       block('saturation', 'dbLatency'),
    db_collection:    block('saturation', 'dbCollection'),
    db_system:        block('saturation', 'dbSystem'),
    db_systems:       block('saturation', 'dbSystems'),
    db_summary:       block('saturation', 'dbSummary'),
    db_slow_queries:  block('saturation', 'dbSlowQueries'),
    db_connections:   block('saturation', 'dbConnections'),

    infra_cpu:           block('infrastructure', 'infraCPU'),
    infra_memory:        block('infrastructure', 'infraMemory'),
    infra_jvm:           block('infrastructure', 'infraJVM'),
    infra_disk:          block('infrastructure', 'infraDisk'),
    infra_network:       block('infrastructure', 'infraNetwork'),
    infra_connpool:      block('infrastructure', 'infraConnPool'),
    infra_nodes:         block('infrastructure', 'infraNodes'),
    infra_fleet:         block('infrastructure', 'infraFleet'),
    infra_resource_util: block('infrastructure', 'infraResourceUtil'),
    infra_kubernetes:    block('infrastructure', 'infraKubernetes'),
  },
};

export { setup, teardown, handleSummary };
export {
  dbVolume, dbErrors, dbLatency, dbCollection, dbSystem,
  dbSystems, dbSummary, dbSlowQueries, dbConnections,
  infraCPU, infraMemory, infraJVM, infraDisk, infraNetwork, infraConnPool,
  infraNodes, infraFleet, infraResourceUtil, infraKubernetes,
};
