// k6 entrypoint: logs scenarios.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { logsExplorer }  from '../scenarios/logs/explorer.js';
import { logsDetail }    from '../scenarios/logs/detail.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'logs' },
  };
}

export const options = {
  scenarios: {
    logs_explorer:  block('logsExplorer'),
    logs_detail:    block('logsDetail'),
  },
};

export { setup, teardown, handleSummary };
export { logsExplorer, logsDetail };
