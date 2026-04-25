// k6 entrypoint: overview scenarios (summary, errors, span_errors, slo,
// red metrics, http metrics, apm).

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { overviewSummary }    from '../scenarios/overview/summary.js';
import { overviewErrors }     from '../scenarios/overview/errors.js';
import { overviewSpanErrors } from '../scenarios/overview/span_errors.js';
import { overviewSlo }        from '../scenarios/overview/slo.js';
import { overviewRedMetrics } from '../scenarios/overview/redmetrics.js';
import { overviewHttpMetrics } from '../scenarios/overview/httpmetrics.js';
import { overviewApm }        from '../scenarios/overview/apm.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'overview' },
  };
}

export const options = {
  scenarios: {
    overview_summary:      block('overviewSummary'),
    overview_errors:       block('overviewErrors'),
    overview_span_errors:  block('overviewSpanErrors'),
    overview_slo:          block('overviewSlo'),
    overview_red_metrics:  block('overviewRedMetrics'),
    overview_http_metrics: block('overviewHttpMetrics'),
    overview_apm:          block('overviewApm'),
  },
};

export { setup, teardown, handleSummary };
export {
  overviewSummary, overviewErrors, overviewSpanErrors, overviewSlo,
  overviewRedMetrics, overviewHttpMetrics, overviewApm,
};
