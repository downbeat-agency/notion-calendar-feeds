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
  assert.match(source, /loadFrozenPersonalCalendarHistory\(personId\)/u);
  assert.match(source, /mergeCalendarEventsAcrossHistoryCutover\(/u);
  assert.match(source, /id: event\.uid \|\| undefined/u);
  assert.match(source, /dataSource: 'postgres_with_frozen_legacy_history'/u);
  assert.match(source, /events: allCalendarEvents\.map\(publicCalendarEvent\)/u);
  assert.match(source, /delete publicEvent\.comparisonIdentity/u);
});

test('Postgres cutover freezes legacy personal history without writing payroll data', () => {
  assert.match(source, /calendar:legacy-history:v1:/u);
  assert.match(source, /await redis\.set\(frozenPersonalHistoryCacheKey\(personId\)/u);
  assert.match(source, /LEGACY_CALENDAR_HISTORY_MISSING/u);
  assert.doesNotMatch(source, /insert into payroll_entries|update payroll_entries/u);
});

test('Notion baseline regeneration repairs formula JSON before freezing history', () => {
  assert.match(source, /return parseNotionFormulaJsonArray\(raw \|\| '\[\]'\)/u);
  assert.match(source, /parseJsonFormulaArray\(calendarData\.Flights, 'Flights'\)/u);
  assert.match(source, /parseJsonFormulaArray\(\s*calendarData\['Event Notes Reminders'\]/u);
  assert.match(source, /readStableFormulaSnapshot\(/u);
  assert.match(source, /NOTION_EVENT_FORMULA_STABILITY_ATTEMPTS/u);
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
    '/api/internal/calendar-shadow-diff/:personId',
  ]) {
    const routeIndex = source.indexOf(`'${route}'`);
    assert.notEqual(routeIndex, -1);
    assert.match(source.slice(routeIndex, routeIndex + 150), /requireCalendarFeedServiceKey/u);
  }
  assert.match(source, /processCalendarDataIndexEntries\(/u);
  assert.match(source, /compareCalendarShadowSweepResult\(refreshResult\)/u);
  assert.match(source, /refreshAndCompareSharedCalendarShadow\(/u);
  assert.match(source, /phase: 'refreshing_personal_baselines'/u);
  assert.match(source, /SHADOW_BASELINE_CACHE_MISSING/u);
  assert.match(source, /pageSize: 100,\s+pagesPerRun: 1000/u);
  assert.match(source, /summary\.pairsByMethod\[method\]/u);
});

test('full shadow audits refresh durable Notion baselines before comparison', () => {
  assert.match(source, /persistCalendarShadowBaseline\(redis/u);
  assert.match(source, /loadCalendarShadowBaseline\(redis, kind, selector\)/u);
  assert.match(source, /trigger: 'shadow_baseline_audit'/u);
  assert.match(source, /const refreshBaselines = req\.query\.refresh !== 'false'/u);
  assert.match(source, /phase: 'durable_personal_comparison'/u);
  assert.match(source, /allowEmptyBaselines: CALENDAR_FEED_SOURCE === 'shadow'/u);
  assert.match(source, /saveCalendarShadowBaseline\(\s*'personal'/u);
  assert.match(source, /activeManualRegens \+= 1/u);
  assert.match(source, /activeManualRegens = Math\.max\(0, activeManualRegens - 1\)/u);
});
