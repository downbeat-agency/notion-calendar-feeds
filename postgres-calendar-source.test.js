import test from 'node:test';
import assert from 'node:assert/strict';
import {
  calendarFeedServiceRequestIsAuthorized,
  compareCalendarEventSets,
  configuredCalendarFeedSource,
  fetchPostgresCalendarFeed,
} from './postgres-calendar-source.js';

test('calendar shadow report authentication uses the dedicated service key', () => {
  const req = { get: () => 'secret' };
  assert.equal(calendarFeedServiceRequestIsAuthorized(req, { CALENDAR_FEED_SERVICE_KEY: 'secret' }), true);
  assert.equal(calendarFeedServiceRequestIsAuthorized(req, { CALENDAR_FEED_SERVICE_KEY: 'wrong' }), false);
});

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

test('shadow comparison separates missing occurrences from field-level drift', () => {
  const notion = [{
    type: 'main_event',
    title: 'Event',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
    description: 'Legacy description',
    location: 'Venue',
    url: 'https://notion.test/event',
  }];
  const postgres = [{ ...notion[0], description: 'Postgres description' }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.matches, false);
  assert.equal(comparison.pairedCount, 1);
  assert.equal(comparison.unpairedNotionCount, 0);
  assert.equal(comparison.unpairedPostgresCount, 0);
  assert.equal(comparison.fieldMismatchCounts.description, 1);
  assert.doesNotMatch(JSON.stringify(comparison), /Legacy description|Postgres description/u);
});

test('shadow comparison pairs the same main event despite presentation differences', () => {
  const notion = [{
    type: 'main_event',
    title: 'Legacy title (Legacy band label)',
    mainEvent: 'Private Event — Legacy helper',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
  }];
  const postgres = [{
    type: 'main_event',
    title: 'Postgres title (Current band label)',
    mainEvent: 'Private Event',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
  }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.pairedCount, 1);
  assert.equal(comparison.pairsByMethod.containedMainEvent, 1);
  assert.equal(comparison.fieldMismatchCounts.title, 1);
  assert.equal(comparison.unpairedNotionCount, 0);
  assert.equal(comparison.unpairedPostgresCount, 0);
});
