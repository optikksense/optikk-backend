// Static value pools used to vary payloads across iterations. Tuned for the
// OTel demo data the local stack typically holds; if your dev DB has
// different services, override via the scenario-level filter overrides.

export const services = [
  'frontend', 'cart', 'checkout', 'payment',
  'shipping', 'product-catalog', 'recommendation', 'ad',
];

export const severities = ['INFO', 'WARN', 'ERROR', 'DEBUG'];
export const severityNumbers = ['9', '13', '17', '5'];

export const httpMethods = ['GET', 'POST', 'PUT', 'DELETE'];
export const httpStatuses = ['200', '201', '400', '401', '403', '404', '500', '503'];

export const spanKinds = ['SERVER', 'CLIENT', 'INTERNAL', 'PRODUCER', 'CONSUMER'];

export const metricNames = [
  'http.server.duration',
  'http.client.duration',
  'rpc.server.duration',
  'db.client.operation.duration',
  'process.runtime.go.gc.count',
  'process.runtime.go.mem.heap_alloc',
];

export const attributeKeys = [
  '@http.target', '@http.route', '@http.method', '@http.status_code',
  '@db.system', '@db.statement', '@rpc.service',
  '@user.id', '@request.id', '@deployment.environment',
];

export const environments = ['dev', 'staging', 'prod'];

export const errorGroupSampleIds = [
  'errgrp_demo_1', 'errgrp_demo_2', 'errgrp_demo_3',
];

export const kafkaTopics = [
  'optikk.spans.v1', 'optikk.logs.v1', 'optikk.metrics.v1',
];

export const kafkaGroups = [
  'optikk-spans-consumer', 'optikk-logs-consumer', 'optikk-metrics-consumer',
];

export const datastoreSystems = ['mysql', 'redis', 'clickhouse'];

export function randomPick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export function randomPickN(arr, n) {
  if (n >= arr.length) return arr.slice();
  const copy = arr.slice();
  for (let i = copy.length - 1; i > copy.length - 1 - n; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(copy.length - n);
}
