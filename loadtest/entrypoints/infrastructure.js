// k6 entrypoint: infrastructure scenarios.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { infraNodes }       from '../scenarios/infrastructure/nodes.js';
import { infraFleet }       from '../scenarios/infrastructure/fleet.js';
import { infraCompute }     from '../scenarios/infrastructure/compute.js';
import { infraIO }          from '../scenarios/infrastructure/io.js';
import { infraResourceUtil } from '../scenarios/infrastructure/resource_util.js';
import { infraKubernetes }  from '../scenarios/infrastructure/kubernetes.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'infrastructure' },
  };
}

export const options = {
  scenarios: {
    infra_nodes:        block('infraNodes'),
    infra_fleet:        block('infraFleet'),
    infra_compute:      block('infraCompute'),
    infra_io:           block('infraIO'),
    infra_resource_util: block('infraResourceUtil'),
    infra_kubernetes:   block('infraKubernetes'),
  },
};

export { setup, teardown, handleSummary };
export { infraNodes, infraFleet, infraCompute, infraIO, infraResourceUtil, infraKubernetes };
