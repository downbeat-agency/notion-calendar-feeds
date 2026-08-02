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
});

test('Postgres mode does not run the Notion fleet sweep', () => {
  assert.match(
    source,
    /function startBackgroundJob\(\) \{\s+if \(CALENDAR_FEED_SOURCE === 'postgres'\)/u
  );
});

