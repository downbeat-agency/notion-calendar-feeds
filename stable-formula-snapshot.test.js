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

test('formula stability can sample all attempts concurrently', async () => {
  let inFlight = 0;
  let maximumInFlight = 0;
  let pauseCalls = 0;

  const result = await readStableFormulaSnapshot(
    async () => {
      inFlight += 1;
      maximumInFlight = Math.max(maximumInFlight, inFlight);
      await new Promise((resolve) => setImmediate(resolve));
      inFlight -= 1;
      return { events: ['complete'], events2: [] };
    },
    {
      attempts: 3,
      parallel: true,
      scoreSnapshot: score,
      pause: async () => { pauseCalls += 1; },
    }
  );

  assert.equal(maximumInFlight, 3);
  assert.equal(pauseCalls, 0);
  assert.deepEqual(result, { events: ['complete'], events2: [] });
});

test('formula stability can require two matching reads when independently witnessed', async () => {
  const complete = { events: ['complete'], events2: [] };
  const result = await readStableFormulaSnapshot(
    reader([complete, complete]),
    { attempts: 2, scoreSnapshot: score }
  );
  assert.deepEqual(result, complete);
});
