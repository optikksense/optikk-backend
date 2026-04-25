// k6 entrypoint: saturation scenarios (datastores + kafka).

import { setup, teardown } from '../lib/bootstrap.js';
import { handleSummary } from '../lib/summary.js';
import { cfg } from '../lib/config.js';

import { saturationDatastoresExplorer }  from '../scenarios/saturation/datastores_explorer.js';
import { saturationDatastoresDrilldown } from '../scenarios/saturation/datastores_drilldown.js';
import { saturationKafkaExplorer }       from '../scenarios/saturation/kafka_explorer.js';
import { saturationKafkaPerf }           from '../scenarios/saturation/kafka_perf.js';
import { saturationKafkaLag }            from '../scenarios/saturation/kafka_lag.js';
import { saturationKafkaHealth }         from '../scenarios/saturation/kafka_health.js';

function block(name) {
  return {
    executor: 'constant-arrival-rate',
    exec: name,
    rate: cfg.rps, timeUnit: '1s',
    duration: cfg.duration, preAllocatedVUs: cfg.vus,
    tags: { module: 'saturation' },
  };
}

export const options = {
  scenarios: {
    saturation_datastores_explorer:  block('saturationDatastoresExplorer'),
    saturation_datastores_drilldown: block('saturationDatastoresDrilldown'),
    saturation_kafka_explorer:       block('saturationKafkaExplorer'),
    saturation_kafka_perf:           block('saturationKafkaPerf'),
    saturation_kafka_lag:            block('saturationKafkaLag'),
    saturation_kafka_health:         block('saturationKafkaHealth'),
  },
};

export { setup, teardown, handleSummary };
export {
  saturationDatastoresExplorer, saturationDatastoresDrilldown,
  saturationKafkaExplorer, saturationKafkaPerf, saturationKafkaLag, saturationKafkaHealth,
};
