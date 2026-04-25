// Standard assertions applied by lib/client.js. Returns the parsed body if
// the envelope is valid, or null otherwise — callers use the returned value
// to chain follow-up requests.

import { check } from 'k6';
import { reqErrors, reqSuccess, envelopeFailures } from './metrics.js';
import { categorizeError } from './errors.js';

export function assertOk(res, tags) {
  const ctype = res.headers && (res.headers['Content-Type'] || res.headers['content-type']) || '';
  const ok = check(res, {
    'status is 200':       (r) => r.status === 200,
    'content-type is json': () => ctype.indexOf('application/json') === 0,
  }, tags);

  if (!ok) {
    reqErrors.add(1, { ...tags, kind: categorizeError(res) });
    reqSuccess.add(false, tags);
    return null;
  }

  let body;
  try {
    body = res.json();
  } catch (_e) {
    envelopeFailures.add(1, tags);
    reqErrors.add(1, { ...tags, kind: 'envelope' });
    reqSuccess.add(false, tags);
    return null;
  }

  const envelopeOk = check(body, {
    'envelope success=true': (b) => b && b.success === true,
  }, tags);

  if (!envelopeOk) {
    envelopeFailures.add(1, tags);
    reqErrors.add(1, { ...tags, kind: 'envelope' });
    reqSuccess.add(false, tags);
    return null;
  }

  reqSuccess.add(true, tags);
  return body;
}
