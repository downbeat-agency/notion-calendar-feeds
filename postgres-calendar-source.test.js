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

test('shadow comparison recognizes equivalent Notion URL forms without hiding exact drift', () => {
  const notion = [{
    type: 'hotel',
    title: 'Hotel',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-02T10:00:00Z',
    description: 'Notion Link: https://www.notion.so/Private-Hotel-123456781234123412341234567890ab',
    url: 'https://www.notion.so/Private-Hotel-123456781234123412341234567890ab',
  }];
  const postgres = [{
    ...notion[0],
    description: 'Notion Link: https://app.notion.com/Private-Hotel-123456781234123412341234567890ab?pvs=4',
    url: 'https://app.notion.com/Private-Hotel-123456781234123412341234567890ab?pvs=4',
  }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.matches, false);
  assert.equal(comparison.semanticMatches, true);
  assert.equal(comparison.pairedCount, 1);
  assert.equal(comparison.pairsByMethod.url, 1);
  assert.equal(comparison.fieldMismatchCounts.url, 1);
  assert.equal(comparison.semanticFieldMismatchCounts.url, 0);
  assert.equal(comparison.semanticFieldMismatchCounts.description, 0);
});

test('shadow comparison does not canonicalize unrelated URLs that contain a 32-character id', () => {
  const notion = [{
    type: 'main_event',
    title: 'Event',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
    description: 'Link: https://example.com/123456781234123412341234567890ab',
  }];
  const postgres = [{
    ...notion[0],
    description: 'Link: https://other.example/123456781234123412341234567890ab',
  }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.semanticMatches, false);
  assert.equal(comparison.semanticFieldMismatchCounts.description, 1);
});

test('shadow comparison reports unpaired and field drift by occurrence type', () => {
  const notion = [{
    type: 'main_event',
    title: 'Legacy Event',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
  }, {
    type: 'rehearsal',
    title: 'Rehearsal',
    start: '2026-08-02T10:00:00Z',
    end: '2026-08-02T11:00:00Z',
  }];
  const postgres = [{ ...notion[0], title: 'Current Event' }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.unpairedNotionByType.rehearsal, 1);
  assert.equal(comparison.unpairedPostgresByType.rehearsal, undefined);
  assert.equal(comparison.fieldMismatchCountsByType.main_event.title, 1);
  assert.equal(comparison.semanticFieldMismatchCountsByType.main_event.title, 1);
});

test('shadow exact comparison treats duplicate occurrences as a multiset', () => {
  const event = {
    type: 'team_calendar',
    title: 'Office',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T11:00:00Z',
  };
  const comparison = compareCalendarEventSets([event, { ...event }], [event]);
  assert.equal(comparison.matches, false);
  assert.equal(comparison.missingFromPostgresCount, 1);
  assert.equal(comparison.extraInPostgresCount, 0);
});

test('shadow comparison pairs repeated titles to the nearest occurrence time', () => {
  const event = (start) => ({
    type: 'rehearsal',
    title: 'Rehearsal',
    start,
    end: start,
  });
  const notion = [event('2026-08-01T10:00:00Z'), event('2026-08-08T10:00:00Z')];
  const postgres = [
    event('2026-07-25T10:00:00Z'),
    event('2026-08-01T10:00:00Z'),
    event('2026-08-08T10:00:00Z'),
  ];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.pairedCount, 2);
  assert.equal(comparison.fieldMismatchCounts.start, 0);
  assert.equal(comparison.unpairedPostgresByType.rehearsal, 1);
});

test('shadow comparison does not pair weak repeated labels across distant dates', () => {
  const notion = [{
    type: 'main_event',
    title: 'Same City Wedding',
    mainEvent: 'Same City Wedding',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T12:00:00Z',
  }];
  const postgres = [{
    type: 'main_event',
    title: 'Same City Wedding',
    mainEvent: 'Same City Wedding',
    start: '2026-08-08T10:00:00Z',
    end: '2026-08-08T12:00:00Z',
  }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.pairedCount, 0);
  assert.equal(comparison.unpairedNotionByType.main_event, 1);
  assert.equal(comparison.unpairedPostgresByType.main_event, 1);
});

test('shadow comparison uses embedded Notion links as strong occurrence identities', () => {
  const pageId = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
  const notion = [{
    type: 'main_event',
    title: 'Legacy title',
    start: '2026-08-01T10:00:00Z',
    end: '2026-08-01T12:00:00Z',
    description: `Notion Link: https://www.notion.so/${pageId}`,
  }];
  const postgres = [{
    type: 'main_event',
    title: 'Postgres title',
    start: '2026-08-08T10:00:00Z',
    end: '2026-08-08T12:00:00Z',
    description: `Notion Link: https://notion.so/workspace-${pageId.replaceAll('-', '')}`,
  }];
  const comparison = compareCalendarEventSets(notion, postgres);
  assert.equal(comparison.pairedCount, 1);
  assert.equal(comparison.pairsByMethod.descriptionUrl, 1);
});
