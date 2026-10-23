const assert = require('node:assert/strict');
const { test } = require('node:test');
const { acknowledgedClose, closeOwned } = require('../close-owned.ts');
test('partially acquired resources close in reverse lifetime order', async () => {
  const calls = [];
  const owned = [];
  await assert.rejects(async () => {
    try {
      owned.push({
        name: 'database',
        close: async () => calls.push('database'),
      });
      owned.push({ name: 'sync', close: async () => calls.push('sync') });
      throw new Error('second sync open failed');
    } finally {
      assert.deepEqual(await closeOwned(owned), []);
    }
  }, /second sync open failed/);
  assert.deepEqual(calls, ['sync', 'database']);
});
test('failed sibling close does not skip sync or database close', async () => {
  const calls = [];
  const errors = await closeOwned(
    ['database', 'sync', 'sibling'].map(name => ({
      name,
      close: async () => {
        calls.push(name);
        if (name === 'sibling') throw new Error('close failed');
      },
    })),
  );
  assert.deepEqual(calls, ['sibling', 'sync', 'database']);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /^sibling:.*close failed/);
});

test('failed disposable close remains owned for cleanup; acknowledged close is not repeated', async () => {
  let calls = 0;
  const disposable = acknowledgedClose('disposable', async () => {
    calls++;
    if (calls === 1) throw new Error('release not acknowledged');
  });
  const owned = [disposable];
  await assert.rejects(disposable.close(), /not acknowledged/);
  assert.deepEqual(await closeOwned(owned), []);
  assert.equal(calls, 2);
  assert.deepEqual(await closeOwned(owned), []);
  assert.equal(calls, 2);
});
