import test from 'node:test';
import assert from 'node:assert/strict';
import { readStableFormulaSnapshot } from './stable-formula-snapshot.js';

function reader(values) {
  let index = 0;
  return async () => values[index++];
}

const score = (snapshot) => snapshot.events.length + snapshot.events2.length;

test('formula stability selects the repeated fullest snapshot', async () => {
  const full = { events: ['a'], events2: ['b'] };
  const result = await readStableFormulaSnapshot(
    reader([{ events: [], events2: [] }, full, full]),
    { scoreSnapshot: score }
  );
  assert.deepEqual(result, full);
});

test('formula stability rejects a repeated partial read when a fuller read was observed', async () => {
  const partial = { events: [], events2: ['b'] };
  const full = { events: ['a'], events2: ['b'] };
  await assert.rejects(
    readStableFormulaSnapshot(reader([partial, full, partial]), { scoreSnapshot: score }),
    (error) => error.code === 'NOTION_CALENDAR_FORMULA_UNSTABLE'
  );
});

test('formula stability accepts a consistently empty calendar', async () => {
  const empty = { events: [], events2: [] };
  const result = await readStableFormulaSnapshot(reader([empty, empty, empty]), {
    scoreSnapshot: score,
  });
  assert.deepEqual(result, empty);
});
