// Single read-and-validate point for all k6 --env flags. Importing this
// module from a scenario gives you `cfg` already validated; the test aborts
// in setup() if anything is malformed.

const env = (typeof __ENV !== 'undefined') ? __ENV : {};

function requireEmail(s) {
  if (!s || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(s)) {
    throw new Error(`config: EMAIL is invalid or missing (got: ${JSON.stringify(s)})`);
  }
  return s;
}

function requireDuration(s) {
  if (!/^\d+(ms|s|m|h)$/.test(s)) {
    throw new Error(`config: DURATION must look like 30s, 5m, 1h (got: ${JSON.stringify(s)})`);
  }
  return s;
}

function requirePositiveInt(name, s) {
  const n = Number(s);
  if (!Number.isFinite(n) || n <= 0 || Math.floor(n) !== n) {
    throw new Error(`config: ${name} must be a positive integer (got: ${JSON.stringify(s)})`);
  }
  return n;
}

function requireLookback(s) {
  const allowed = new Set(['5m', '15m', '1h', '6h', '24h', '7d']);
  if (!allowed.has(s)) {
    throw new Error(`config: LOOKBACK must be one of ${[...allowed].join('|')} (got: ${JSON.stringify(s)})`);
  }
  return s;
}

export const cfg = {
  baseUrl:   (env.BASE_URL   || 'http://localhost:19090').replace(/\/+$/, ''),
  email:     requireEmail(env.EMAIL || 'loadtest@optikk.local'),
  password:  env.PASSWORD || 'optikk-loadtest',
  teamName:  env.TEAM_NAME || 'Loadtest Team',
  orgName:   env.ORG_NAME  || 'Loadtest Org',
  teamId:    env.TEAM_ID ? requirePositiveInt('TEAM_ID', env.TEAM_ID) : undefined,
  rps:       requirePositiveInt('RPS', env.RPS || '10'),
  duration:  requireDuration(env.DURATION || '1m'),
  vus:       requirePositiveInt('VUS', env.VUS || '50'),
  jsonOut:   env.JSON_OUT || undefined,
  lookback:  requireLookback(env.LOOKBACK || '1h'),
  allowRemoteBootstrap: env.ALLOW_REMOTE_BOOTSTRAP === '1',
};
