// Builders for request body shapes used across scenarios. Each builder is a
// pure function over its inputs so payload construction stays out of the
// scenario default() functions (which need to stay under 40 LOC).

export function tracesQueryBody(o) {
  return {
    startTime: o.startTime,
    endTime:   o.endTime,
    filters:   o.filters || [],
    include:   o.include || ['summary'],
    limit:     o.limit   || 50,
    cursor:    o.cursor  || '',
  };
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
  return {
    startTime: o.startTime,
    endTime:   o.endTime,
    filters:   o.filters || [],
    include:   o.include || ['summary'],
    limit:     o.limit   || 100,
    cursor:    o.cursor  || '',
  };
}

export function tracesAnalyticsBody(o) {
  return {
    startTime:    o.startTime,
    endTime:      o.endTime,
    filters:      o.filters || [],
    groupBy:      o.groupBy || ['service'],
    aggregations: o.aggregations || [{ fn: 'count', alias: 'count' }],
    step:         o.step    || '1m',
    vizMode:      o.vizMode || 'timeseries',
    limit:        o.limit   || 50,
    orderBy:      o.orderBy || 'count desc',
  };
}

export function logsAnalyticsBody(o) {
  return {
    startTime:    o.startTime,
    endTime:      o.endTime,
    filters:      o.filters || [],
    groupBy:      o.groupBy || ['severity_text'],
    aggregations: o.aggregations || [{ fn: 'count', alias: 'count' }],
    step:         o.step    || '1m',
    vizMode:      o.vizMode || 'timeseries',
    limit:        o.limit   || 50,
    orderBy:      o.orderBy || 'count desc',
  };
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
  return {
    metricNames: o.metricNames,
    tagFilters:  o.tagFilters || {},
    aggregation: o.aggregation || 'avg',
    startTime:   o.startTime,
    endTime:     o.endTime,
    step:        o.step || '60s',
  };
}
