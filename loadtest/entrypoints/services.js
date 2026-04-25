// k6 entrypoint: services scenarios (topology + deployments).

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { servicesTopology }   from '../scenarios/services/topology.js';
import { servicesDeployments } from '../scenarios/services/deployments.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'services' },
  };
}

export const options = {
  scenarios: {
    services_topology:    block('servicesTopology'),
    services_deployments: block('servicesDeployments'),
  },
};

export { setup, teardown, handleSummary };
export { servicesTopology, servicesDeployments };
