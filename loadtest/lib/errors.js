// Categorize a failed response so the JSON summary rolls up by failure mode.

export function categorizeError(res) {
  if (!res) return 'unknown';
  if (res.error_code === 1050) return 'timeout';
  if (res.error_code >= 1000 && res.error_code < 2000) return 'network';
  const s = res.status || 0;
  if (s >= 500) return '5xx';
  if (s >= 400) return '4xx';
  if (s === 200) return 'envelope';
  return 'other';
}
