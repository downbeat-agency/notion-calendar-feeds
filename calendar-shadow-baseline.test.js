import test from 'node:test';
import assert from 'node:assert/strict';
import {
  calendarShadowBaselineKey,
  loadCalendarShadowBaseline,
  persistCalendarShadowBaseline,
} from './calendar-shadow-baseline.js';

function memoryRedis() {
  const values = new Map();
  return {
    values,
    async get(key) { return values.get(key) || null; },
    async set(key, value) { values.set(key, value); return 'OK'; },
  };
}

test('shadow baselines use a durable Redis write and retain the captured events', async () => {
  const redis = memoryRedis();
  const capturedAt = '2026-08-03T04:00:00.000Z';
  await persistCalendarShadowBaseline(redis, {
    kind: 'personal',
    selector: 'PERSON-ID',
    sourcePageId: 'calendar-data-row',
    capturedAt,
    events: [{ type: 'main_event', start: capturedAt }],
  });

  const key = calendarShadowBaselineKey('personal', 'person-id');
  assert.equal(redis.values.has(key), true);
  const loaded = await loadCalendarShadowBaseline(redis, 'personal', 'PERSON-ID');
  assert.equal(loaded.capturedAt, capturedAt);
  assert.equal(loaded.sourcePageId, 'calendar-data-row');
  assert.deepEqual(loaded.events, [{ type: 'main_event', start: capturedAt }]);
});

test('shadow baseline reads distinguish missing and malformed snapshots', async () => {
  const redis = memoryRedis();
  await assert.rejects(
    loadCalendarShadowBaseline(redis, 'travel', 'travel'),
    (error) => error.code === 'SHADOW_BASELINE_CACHE_MISSING'
  );
  redis.values.set(calendarShadowBaselineKey('travel', 'travel'), '{broken');
  await assert.rejects(
    loadCalendarShadowBaseline(redis, 'travel', 'travel'),
    (error) => error.code === 'SHADOW_BASELINE_CACHE_INVALID'
  );
});
