// k6 entrypoint: metrics scenarios.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { metricsQuery } from '../scenarios/metrics/query.js';
import { metricsMeta }  from '../scenarios/metrics/meta.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'metrics' },
  };
}

export const options = {
  scenarios: {
    metrics_query: block('metricsQuery'),
    metrics_meta:  block('metricsMeta'),
  },
};

export { setup, teardown, handleSummary };
export { metricsQuery, metricsMeta };
