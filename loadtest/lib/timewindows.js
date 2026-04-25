// Time-window helpers for query payloads. `windowFor` snaps to a 1-minute
// bucket so iterations hit Redis cache; `randomWindow` jitters within the
// last 24h so a fraction of iterations bypass the cache.

const SPANS_MS = {
  '5m':  5 * 60 * 1000,
  '15m': 15 * 60 * 1000,
  '1h':  60 * 60 * 1000,
  '6h':  6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d':  7 * 24 * 60 * 60 * 1000,
};

function spanMs(lookback) {
  const v = SPANS_MS[lookback];
  if (!v) throw new Error(`timewindows: unsupported lookback ${lookback}`);
  return v;
}

export function windowFor(lookback) {
  const span = spanMs(lookback);
  const endTime = Math.floor(Date.now() / 60000) * 60000;
  return { startTime: endTime - span, endTime };
}

export function randomWindow(lookback) {
  const span = spanMs(lookback);
  const jitterMax = SPANS_MS['24h'] - span;
  const jitter = Math.max(0, Math.floor(Math.random() * jitterMax));
  const endTime = Date.now() - jitter;
  return { startTime: endTime - span, endTime };
}

// 70% hot (cache-friendly), 30% cold (cache-busting). Per-iteration coin flip.
export function adaptiveWindow(lookback) {
  return Math.random() < 0.3 ? randomWindow(lookback) : windowFor(lookback);
}
