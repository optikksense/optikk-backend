// Custom k6 metrics so the Prometheus output groups by {module, endpoint}.
// k6 picks these up automatically and exports them under their declared
// names — no further wiring required.

import { Trend, Counter, Rate } from 'k6/metrics';

export const reqDuration = new Trend('loadtest_request_duration_ms', true);
export const reqErrors   = new Counter('loadtest_request_errors_total');
export const reqSuccess  = new Rate('loadtest_request_success_rate');
export const envelopeFailures = new Counter('loadtest_envelope_failures_total');
