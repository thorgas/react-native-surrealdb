const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { test } = require('node:test');
const captureMetro = require('../capture-metro.cjs');
function exercise(callback) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bounded-tee-test-'));
  const previous = process.env.BOUNDED_CAPTURE_DIRECTORY;
  process.env.BOUNDED_CAPTURE_DIRECTORY = directory;
  const response = new EventEmitter();
  const calls = [];
  response.statusCode = 200;
  response.getHeaders = () => ({ 'content-type': 'application/javascript' });
  response.write = function (...args) {
    calls.push(['write', ...args]);
    return false;
  };
  response.end = function (...args) {
    calls.push(['end', ...args]);
    this.writableFinished = true;
    this.emit('finish');
    return this;
  };
  try {
    captureMetro((_req, res) => callback(res, calls))(
      { url: '/test.bundle?modulesOnly=true', headers: {} },
      response,
    );
    return {
      calls,
      body: fs.readFileSync(path.join(directory, '001.body')),
      receipt: JSON.parse(fs.readFileSync(path.join(directory, '001.json'))),
    };
  } finally {
    if (previous === undefined) delete process.env.BOUNDED_CAPTURE_DIRECTORY;
    else process.env.BOUNDED_CAPTURE_DIRECTORY = previous;
    fs.rmSync(directory, { recursive: true });
  }
}
test('callback-only end is forwarded exactly without capturing callback', () => {
  const done = () => {};
  const result = exercise((res, calls) => {
    assert.equal(res.end(done), res);
    assert.equal(calls[0][1], done);
  });
  assert.equal(result.body.length, 0);
  assert.equal(result.receipt.finished, true);
});
test('write overloads preserve bytes, callback arguments and backpressure return', () => {
  const done = () => {};
  const result = exercise((res, calls) => {
    assert.equal(res.write('ä', 'utf8', done), false);
    assert.equal(res.write(Buffer.from([0, 255]), done), false);
    assert.equal(res.write(new Uint8Array([1, 2]), done), false);
    assert.equal(res.end('!', done), res);
    assert.deepEqual(
      calls.map(x => x[x.length - 1]),
      [done, done, done, done],
    );
  });
  assert.deepEqual(result.body, Buffer.from([0xc3, 0xa4, 0, 255, 1, 2, 33]));
  assert.equal(result.receipt.bytes, 7);
});
test('empty end and string with encoding end are forwarded', () => {
  assert.equal(exercise(res => res.end()).body.length, 0);
  assert.deepEqual(
    exercise(res => res.end('ff', 'hex')).body,
    Buffer.from([255]),
  );
});
