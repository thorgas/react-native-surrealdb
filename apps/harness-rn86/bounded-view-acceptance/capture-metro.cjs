const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
module.exports = function captureMetro(next) {
  const directory = process.env.BOUNDED_CAPTURE_DIRECTORY;
  if (!directory) throw new Error('BOUNDED_CAPTURE_DIRECTORY is required');
  fs.mkdirSync(directory, { recursive: true });
  let sequence = 0;
  return function capture(req, res, nextMiddleware) {
    if (!new URL(req.url, 'http://localhost').pathname.endsWith('.bundle'))
      return next(req, res, nextMiddleware);
    const id = ++sequence;
    const chunks = [];
    let bytes = 0;
    let failure = id > 16 ? 'body-count-cap' : null;
    const originalWrite = res.write;
    const originalEnd = res.end;
    const append = (chunk, encoding) => {
      if (
        failure ||
        !(typeof chunk === 'string' || chunk instanceof Uint8Array)
      )
        return;
      const value = Buffer.isBuffer(chunk)
        ? Buffer.from(chunk)
        : Buffer.from(
            chunk,
            typeof encoding === 'string' ? encoding : undefined,
          );
      bytes += value.length;
      if (bytes > 64 * 1024 * 1024) {
        failure = 'body-size-cap';
        chunks.length = 0;
        return;
      }
      chunks.push(value);
    };
    res.write = function (...args) {
      append(args[0], args[1]);
      return originalWrite.apply(this, args);
    };
    res.end = function (...args) {
      append(args[0], args[1]);
      return originalEnd.apply(this, args);
    };
    let saved = false;
    const save = finished => {
      if (saved) return;
      saved = true;
      try {
        const body = Buffer.concat(chunks);
        const file = String(id).padStart(3, '0');
        if (!failure)
          fs.writeFileSync(path.join(directory, file + '.body'), body);
        fs.writeFileSync(
          path.join(directory, file + '.json'),
          JSON.stringify(
            {
              id,
              url: req.url,
              requestKind:
                req.headers['x-react-native-harness-request-kind'] ||
                'app-or-other',
              status: res.statusCode,
              headers: res.getHeaders(),
              finished,
              failure,
              bytes,
              sha256: failure
                ? null
                : crypto.createHash('sha256').update(body).digest('hex'),
              representation:
                'server-response-write-bytes; inspect content-encoding; not device-side hash',
            },
            null,
            2,
          ),
        );
      } catch (error) {
        try {
          fs.writeFileSync(
            path.join(directory, 'capture-failure.json'),
            JSON.stringify({ message: String(error) }),
          );
        } catch (_) {
          console.error('Bounded capture receipt failure');
        }
      }
    };
    res.once('finish', () => save(true));
    res.once('close', () => save(res.writableFinished));
    return next(req, res, nextMiddleware);
  };
};
