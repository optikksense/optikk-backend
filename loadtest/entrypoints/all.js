// k6 entrypoint: full sweep across every read scenario in every module.
// Per-scenario RPS is computed from cfg.rps using a fixed weight per module
// (traces 25 / logs 20 / metrics 10 / overview 25 / infrastructure 10 /
// saturation 8 / services 2). Every scenario gets at least 1 RPS.

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { tracesExplorer }     from '../scenarios/traces/explorer.js';
import { tracesAnalytics }    from '../scenarios/traces/analytics.js';
import { spansQuery, tracesSuggest } from '../scenarios/traces/span_query.js';
import { traceDetailCore }    from '../scenarios/traces/detail_core.js';
import { traceDetailExtras }  from '../scenarios/traces/detail_extras.js';
import { tracePathsAndShape } from '../scenarios/traces/paths_and_shape.js';
import { traceErrors }        from '../scenarios/traces/errors.js';
import { traceLatency }       from '../scenarios/traces/latency.js';

import { logsExplorer }  from '../scenarios/logs/explorer.js';
import { logsAnalytics } from '../scenarios/logs/analytics.js';
import { logsDetail }    from '../scenarios/logs/detail.js';

import { metricsQuery } from '../scenarios/metrics/query.js';
import { metricsMeta }  from '../scenarios/metrics/meta.js';

import { overviewSummary }     from '../scenarios/overview/summary.js';
import { overviewErrors }      from '../scenarios/overview/errors.js';
import { overviewSpanErrors }  from '../scenarios/overview/span_errors.js';
import { overviewSlo }         from '../scenarios/overview/slo.js';
import { overviewRedMetrics }  from '../scenarios/overview/redmetrics.js';
import { overviewHttpMetrics } from '../scenarios/overview/httpmetrics.js';
import { overviewApm }         from '../scenarios/overview/apm.js';

import { infraNodes }        from '../scenarios/infrastructure/nodes.js';
import { infraFleet }        from '../scenarios/infrastructure/fleet.js';
import { infraCompute }      from '../scenarios/infrastructure/compute.js';
import { infraIO }           from '../scenarios/infrastructure/io.js';
import { infraResourceUtil } from '../scenarios/infrastructure/resource_util.js';
import { infraKubernetes }   from '../scenarios/infrastructure/kubernetes.js';

import { saturationDatastoresExplorer }  from '../scenarios/saturation/datastores_explorer.js';
import { saturationDatastoresDrilldown } from '../scenarios/saturation/datastores_drilldown.js';
import { saturationKafkaExplorer }       from '../scenarios/saturation/kafka_explorer.js';
import { saturationKafkaPerf }           from '../scenarios/saturation/kafka_perf.js';
import { saturationKafkaLag }            from '../scenarios/saturation/kafka_lag.js';
import { saturationKafkaHealth }         from '../scenarios/saturation/kafka_health.js';

import { servicesTopology }    from '../scenarios/services/topology.js';
import { servicesDeployments } from '../scenarios/services/deployments.js';

const WEIGHTS = {
  traces:         { share: 0.25, count: 9 },
  logs:           { share: 0.20, count: 3 },
  metrics:        { share: 0.10, count: 2 },
  overview:       { share: 0.25, count: 7 },
  infrastructure: { share: 0.10, count: 6 },
  saturation:     { share: 0.08, count: 6 },
  services:       { share: 0.02, count: 2 },
};

function rateFor(module) {
  const w = WEIGHTS[module];
  return Math.max(1, Math.floor((cfg.rps * w.share) / w.count));
}

function block(module, execName) {
  return {
    executor: 'constant-arrival-rate',
    exec: execName,
    rate: rateFor(module), timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module },
  };
}

export const options = {
  scenarios: {
    traces_explorer:        block('traces', 'tracesExplorer'),
    traces_analytics:       block('traces', 'tracesAnalytics'),
    traces_spans_query:     block('traces', 'spansQuery'),
    traces_suggest:         block('traces', 'tracesSuggest'),
    traces_detail_core:     block('traces', 'traceDetailCore'),
    traces_detail_extras:   block('traces', 'traceDetailExtras'),
    traces_paths_and_shape: block('traces', 'tracePathsAndShape'),
    traces_errors:          block('traces', 'traceErrors'),
    traces_latency:         block('traces', 'traceLatency'),

    logs_explorer:  block('logs', 'logsExplorer'),
    logs_analytics: block('logs', 'logsAnalytics'),
    logs_detail:    block('logs', 'logsDetail'),

    metrics_query: block('metrics', 'metricsQuery'),
    metrics_meta:  block('metrics', 'metricsMeta'),

    overview_summary:      block('overview', 'overviewSummary'),
    overview_errors:       block('overview', 'overviewErrors'),
    overview_span_errors:  block('overview', 'overviewSpanErrors'),
    overview_slo:          block('overview', 'overviewSlo'),
    overview_red_metrics:  block('overview', 'overviewRedMetrics'),
    overview_http_metrics: block('overview', 'overviewHttpMetrics'),
    overview_apm:          block('overview', 'overviewApm'),

    infra_nodes:         block('infrastructure', 'infraNodes'),
    infra_fleet:         block('infrastructure', 'infraFleet'),
    infra_compute:       block('infrastructure', 'infraCompute'),
    infra_io:            block('infrastructure', 'infraIO'),
    infra_resource_util: block('infrastructure', 'infraResourceUtil'),
    infra_kubernetes:    block('infrastructure', 'infraKubernetes'),

    saturation_datastores_explorer:  block('saturation', 'saturationDatastoresExplorer'),
    saturation_datastores_drilldown: block('saturation', 'saturationDatastoresDrilldown'),
    saturation_kafka_explorer:       block('saturation', 'saturationKafkaExplorer'),
    saturation_kafka_perf:           block('saturation', 'saturationKafkaPerf'),
    saturation_kafka_lag:            block('saturation', 'saturationKafkaLag'),
    saturation_kafka_health:         block('saturation', 'saturationKafkaHealth'),

    services_topology:    block('services', 'servicesTopology'),
    services_deployments: block('services', 'servicesDeployments'),
  },
};

export { setup, teardown, handleSummary };
export {
  tracesExplorer, tracesAnalytics, spansQuery, tracesSuggest,
  traceDetailCore, traceDetailExtras, tracePathsAndShape, traceErrors, traceLatency,
  logsExplorer, logsAnalytics, logsDetail,
  metricsQuery, metricsMeta,
  overviewSummary, overviewErrors, overviewSpanErrors, overviewSlo,
  overviewRedMetrics, overviewHttpMetrics, overviewApm,
  infraNodes, infraFleet, infraCompute, infraIO, infraResourceUtil, infraKubernetes,
  saturationDatastoresExplorer, saturationDatastoresDrilldown,
  saturationKafkaExplorer, saturationKafkaPerf, saturationKafkaLag, saturationKafkaHealth,
  servicesTopology, servicesDeployments,
};
