import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');

test('legacy subscription URLs remain registered during the source cutover', () => {
  for (const route of [
    '/calendar/admin.ics',
    '/calendar/admin',
    '/calendar/travel.ics',
    '/calendar/travel',
    '/calendar/blockout.ics',
    '/calendar/blockout',
    '/calendar/google/:personId.ics',
    '/calendar/:personId.ics',
    '/calendar/:personId',
  ]) {
    assert.match(source, new RegExp(`app\\.get\\('${route.replaceAll('/', '\\/').replaceAll('.', '\\.')}['),]`, 'u'));
  }
});

test('Postgres source mode reuses the legacy renderer with stable event IDs', () => {
  assert.match(source, /CALENDAR_FEED_SOURCE === 'postgres'/u);
  assert.match(source, /fetchPostgresCalendarFeed\('personal', personId\)/u);
  assert.match(source, /buildCalendarEventsFromCalendarData\(calendarData\)/u);
  assert.match(source, /id: event\.uid \|\| undefined/u);
  assert.match(source, /dataSource: 'postgres'/u);
  assert.match(source, /events: allCalendarEvents\.map\(publicCalendarEvent\)/u);
  assert.match(source, /delete publicEvent\.comparisonIdentity/u);
});

test('Postgres mode does not run the Notion fleet sweep', () => {
  assert.match(
    source,
    /function startBackgroundJob\(\) \{\s+if \(CALENDAR_FEED_SOURCE === 'postgres'\)/u
  );
});

test('Postgres cache is validated against the current source revision', () => {
  assert.match(source, /fetchPostgresCalendarFeed\('version'\)/u);
  assert.match(source, /validatePostgresCacheRevision\(revisionCacheKey\)/u);
  assert.match(source, /Source revision changed; rebuilding/u);
});

test('shadow parity report and audit runner require service authentication', () => {
  for (const route of [
    '/api/internal/calendar-shadow-report',
    '/api/internal/calendar-shadow-run',
  ]) {
    const routeIndex = source.indexOf(`'${route}'`);
    assert.notEqual(routeIndex, -1);
    assert.match(source.slice(routeIndex, routeIndex + 150), /requireCalendarFeedServiceKey/u);
  }
  assert.match(source, /compareCachedPersonalCalendarShadow\(entry\.personId\)/u);
  assert.match(source, /compareCachedSharedCalendarShadow\('admin'/u);
  assert.match(source, /phase: 'cached_personal_comparison'/u);
  assert.match(source, /SHADOW_BASELINE_CACHE_MISSING/u);
  assert.match(source, /pageSize: 100,\s+pagesPerRun: 1000/u);
  assert.match(source, /summary\.pairsByMethod\[method\]/u);
});
