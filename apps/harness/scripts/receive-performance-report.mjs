import { createServer } from 'node:http';
import { writeFileSync } from 'node:fs';

const REPORT_RECEIVER_PORT = 18_082;
const MAX_REPORT_BYTES = 1_000_000;

const output = process.argv
  .find(value => value.startsWith('--output='))
  ?.slice('--output='.length);

if (!output) {
  throw new Error(
    'Usage: receive-performance-report.mjs --output=<report.json>',
  );
}

const server = createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    response.writeHead(204).end();
    return;
  }
  if (request.method !== 'POST' || request.url !== '/benchmark-report') {
    response.writeHead(404).end();
    return;
  }

  let body = '';
  request.setEncoding('utf8');
  request.on('data', chunk => {
    body += chunk;
    if (body.length > MAX_REPORT_BYTES) {
      request.destroy(new Error('Benchmark report is too large'));
    }
  });
  request.on('end', () => {
    try {
      const report = JSON.parse(body);
      if (
        ![1, 2, 3, 4].includes(report.schemaVersion) ||
        !Array.isArray(report.metrics)
      ) {
        throw new Error('Benchmark report has an unsupported schema');
      }
      writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
      response.writeHead(204).end();
    } catch (cause) {
      console.error(cause);
      response.writeHead(400).end();
    }
  });
});

server.listen(REPORT_RECEIVER_PORT, '127.0.0.1', () => {
  console.log(`Benchmark report receiver listening on ${REPORT_RECEIVER_PORT}`);
});
