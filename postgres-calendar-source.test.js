import test from 'node:test';
import assert from 'node:assert/strict';
import {
  compareCalendarEventSets,
  configuredCalendarFeedSource,
  fetchPostgresCalendarFeed,
} from './postgres-calendar-source.js';

test('calendar feed source defaults to notion and validates explicit values', () => {
  assert.equal(configuredCalendarFeedSource({}), 'notion');
  assert.equal(configuredCalendarFeedSource({ CALENDAR_FEED_SOURCE: ' SHADOW ' }), 'shadow');
  assert.throws(
    () => configuredCalendarFeedSource({ CALENDAR_FEED_SOURCE: 'firebase' }),
    /notion, shadow, or postgres/u
  );
});

test('Postgres source client preserves the selector and uses the dedicated service header', async () => {
  let request;
  const payload = { schemaVersion: 1, source: 'postgres', calendarData: { events: [] } };
  const result = await fetchPostgresCalendarFeed('personal', 'person/id', {
    env: {
      CALENDAR_FEED_API_BASE_URL: 'https://downbeat.test/',
      CALENDAR_FEED_SERVICE_KEY: 'secret',
    },
    fetchFn: async (url, options) => {
      request = { url, options };
      return { ok: true, status: 200, json: async () => payload };
    },
  });
  assert.equal(request.url, 'https://downbeat.test/api/internal/calendar-feeds/personal/person%2Fid');
  assert.equal(request.options.headers['X-Downbeat-Calendar-Service-Key'], 'secret');
  assert.equal(result, payload);
});

test('shadow comparison reports parity without exposing event text', () => {
  const left = [{
    type: 'main_event',
    title: 'Private Wedding',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
    location: 'Private Home',
  }];
  const matching = compareCalendarEventSets(left, [{ ...left[0] }]);
  assert.equal(matching.matches, true);
  const mismatch = compareCalendarEventSets(left, []);
  assert.equal(mismatch.matches, false);
  assert.equal(mismatch.missingFromPostgresCount, 1);
  assert.doesNotMatch(JSON.stringify(mismatch), /Private Wedding|Private Home/u);
});

