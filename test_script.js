import http from 'http';

const data = JSON.stringify({
  startTime: Date.now() - 3600000,
  endTime: Date.now(),
  step: '1h',
  services: ['foo'],
  severities: ['WARN']
});

const req = http.request({
  hostname: 'localhost',
  port: 8080,
  path: '/api/v1/logs/trends',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': data.length
  }
}, res => {
  let body = '';
  res.on('data', d => body += d);
  res.on('end', () => console.log(res.statusCode, body));
});

req.on('error', e => console.error(e));
req.write(data);
req.end();
