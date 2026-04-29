// Builders for request body shapes used across scenarios. Each builder is a
// pure function over its inputs so payload construction stays out of the
// scenario default() functions (which need to stay under 40 LOC).

export function tracesQueryBody(o) {
  const body = {
    startTime: o.startTime,
    endTime:   o.endTime,
    include:   o.include || ['summary'],
    limit:     o.limit   || 50,
    cursor:    o.cursor  || '',
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
      if (f.field === 'http_status') body.httpStatuses = [f.value];
      if (f.field === 'trace_id') body.traceId = f.value;
    }
  }
  return body;
}

export function spansQueryBody(o) {
  return {
    startTime: o.startTime,
    endTime:   o.endTime,
    filters:   o.filters || [],
    limit:     o.limit   || 50,
    cursor:    o.cursor  || '',
  };
}

export function logsQueryBody(o) {
  const body = {
    startTime: o.startTime,
    endTime:   o.endTime,
    include:   o.include || ['summary'],
    limit:     o.limit   || 100,
    cursor:    o.cursor  || '',
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
      if (f.field === 'severity_text') body.severities = [f.value];
      if (f.field === 'trace_id') body.traceId = f.value;
    }
  }
  return body;
}

export function logsFacetsBody(o) {
  const body = {
    startTime: o.startTime,
    endTime:   o.endTime,
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
      if (f.field === 'severity_text') body.severities = [f.value];
      if (f.field === 'trace_id') body.traceId = f.value;
    }
  }
  return body;
}

export function logsTrendsBody(o) {
  const body = {
    startTime: o.startTime,
    endTime:   o.endTime,
    step:      o.step || '1m',
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
      if (f.field === 'severity_text') body.severities = [f.value];
      if (f.field === 'trace_id') body.traceId = f.value;
    }
  }
  return body;
}

export function tracesAnalyticsBody(o) {
  const body = {
    startTime:    o.startTime,
    endTime:      o.endTime,
    groupBy:      o.groupBy || ['service'],
    aggregations: o.aggregations || [{ fn: 'count', alias: 'count' }],
    step:         o.step    || '1m',
    vizMode:      o.vizMode || 'timeseries',
    limit:        o.limit   || 50,
    orderBy:      o.orderBy || 'count desc',
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
    }
  }
  return body;
}

export function logsAnalyticsBody(o) {
  const body = {
    startTime:    o.startTime,
    endTime:      o.endTime,
    groupBy:      o.groupBy || ['severity_text'],
    aggregations: o.aggregations || [{ fn: 'count', alias: 'count' }],
    step:         o.step    || '1m',
    vizMode:      o.vizMode || 'timeseries',
    limit:        o.limit   || 50,
    orderBy:      o.orderBy || 'count desc',
  };
  if (o.filters) {
    for (const f of o.filters) {
      if (f.field === 'service') body.services = [f.value];
    }
  }
  return body;
}

export function traceSuggestBody(o) {
  return {
    startTime: o.startTime,
    endTime:   o.endTime,
    field:     o.field,
    prefix:    o.prefix || '',
    limit:     o.limit  || 20,
  };
}

export function metricsQueryBody(o) {
  const metricNames = o.metricNames || [];
  return {
    startTime:   o.startTime,
    endTime:     o.endTime,
    step:        o.step || '60s',
    queries:     metricNames.map((name, i) => ({
      id:          `q${i}`,
      metricName:  name,
      aggregation: o.aggregation || 'avg',
      where:       Object.entries(o.tagFilters || {}).map(([k, v]) => ({
        key:      k,
        operator: 'in',
        value:    Array.isArray(v) ? v : [v],
      })),
      groupBy:     o.groupBy || [],
    })),
  };
}
