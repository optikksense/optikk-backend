// k6 entrypoint: every traces-module scenario, each as its own
// constant-arrival-rate executor running in parallel for cfg.duration.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { tracesExplorer }    from '../scenarios/traces/explorer.js';
import { tracesAnalytics }   from '../scenarios/traces/analytics.js';
import { spansQuery, tracesSuggest } from '../scenarios/traces/span_query.js';
import { traceDetailCore }   from '../scenarios/traces/detail_core.js';
import { traceDetailExtras } from '../scenarios/traces/detail_extras.js';
import { tracePathsAndShape } from '../scenarios/traces/paths_and_shape.js';
import { traceErrors }       from '../scenarios/traces/errors.js';
import { traceLatency }      from '../scenarios/traces/latency.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'traces' },
  };
}

export const options = {
  scenarios: {
    traces_explorer:        block('tracesExplorer'),
    traces_analytics:       block('tracesAnalytics'),
    traces_spans_query:     block('spansQuery'),
    traces_suggest:         block('tracesSuggest'),
    traces_detail_core:     block('traceDetailCore'),
    traces_detail_extras:   block('traceDetailExtras'),
    traces_paths_and_shape: block('tracePathsAndShape'),
    traces_errors:          block('traceErrors'),
    traces_latency:         block('traceLatency'),
  },
};

export { setup, teardown, handleSummary };
export {
  tracesExplorer, tracesAnalytics, spansQuery, tracesSuggest,
  traceDetailCore, traceDetailExtras, tracePathsAndShape,
  traceErrors, traceLatency,
};
