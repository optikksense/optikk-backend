// handleSummary implementation: keeps k6's default stdout summary intact and
// optionally writes the full data blob plus a per-tag rollup to JSON_OUT.

import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';
import { cfg } from './config.js';

function rollupByTag(metrics, tagKey) {
  const out = {};
  if (!metrics) return out;
  for (const name of Object.keys(metrics)) {
    if (!name.startsWith('loadtest_')) continue;
    const m = metrics[name];
    if (!m || !m.values) continue;
    const tagVal = (m.tags && m.tags[tagKey]) || '_';
    out[name] = out[name] || {};
    out[name][tagVal] = m.values;
  }
  return out;
}

export function handleSummary(data) {
  const stdout = textSummary(data, { indent: ' ', enableColors: true });
  const result = { 'stdout': stdout };

  if (cfg.jsonOut) {
    const enriched = {
      generatedAt:    new Date().toISOString(),
      config:         { ...cfg, password: '***' },
      rolledUpByEndpoint: rollupByTag(data.metrics, 'endpoint'),
      rolledUpByModule:   rollupByTag(data.metrics, 'module'),
      raw:            data,
    };
    result[cfg.jsonOut] = JSON.stringify(enriched, null, 2);
  }

  return result;
}
