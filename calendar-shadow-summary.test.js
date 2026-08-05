import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeCalendarShadowEntries } from './calendar-shadow-summary.js';

function comparison(overrides = {}) {
  return {
    matches: false,
    semanticMatches: false,
    notionCount: 4,
    postgresCount: 5,
    notionByType: { main_event: 3, rehearsal: 1 },
    postgresByType: { main_event: 3, rehearsal: 2 },
    missingFromPostgresCount: 2,
    extraInPostgresCount: 3,
    pairedCount: 3,
    pairsByMethod: { sourceIdentity: 2, title: 1 },
    exactPairCount: 1,
    semanticPairCount: 2,
    unpairedNotionCount: 1,
    unpairedPostgresCount: 2,
    unpairedNotionByType: { main_event: 1 },
    unpairedPostgresByType: { rehearsal: 2 },
    fieldMismatchCounts: { title: 2 },
    semanticFieldMismatchCounts: { title: 1 },
    fieldMismatchCountsByType: { main_event: { title: 2 } },
    semanticFieldMismatchCountsByType: { main_event: { title: 1 } },
    ...overrides,
  };
}

test('shadow summary keeps personnel metrics separate from shared calendars', () => {
  const summary = summarizeCalendarShadowEntries([
    { kind: 'personal', comparison: comparison() },
    {
      kind: 'personal',
      comparison: comparison({
        matches: true,
        semanticMatches: true,
        notionCount: 1,
        postgresCount: 1,
        notionByType: { main_event: 1 },
        postgresByType: { main_event: 1 },
        missingFromPostgresCount: 0,
        extraInPostgresCount: 0,
        pairedCount: 1,
        exactPairCount: 1,
        semanticPairCount: 1,
        unpairedNotionCount: 0,
        unpairedPostgresCount: 0,
        unpairedNotionByType: {},
        unpairedPostgresByType: {},
      }),
    },
    {
      kind: 'admin',
      comparison: comparison({
        notionCount: 20,
        postgresCount: 21,
        notionByType: { main_event: 20 },
        postgresByType: { main_event: 21 },
        unpairedNotionCount: 4,
        unpairedPostgresCount: 5,
      }),
    },
  ]);

  assert.equal(summary.comparisons, 3);
  assert.equal(summary.notionEvents, 25);
  assert.equal(summary.byKind.personal.comparisons, 2);
  assert.equal(summary.byKind.personal.matches, 1);
  assert.equal(summary.byKind.personal.notionEvents, 5);
  assert.equal(summary.byKind.personal.postgresEvents, 6);
  assert.deepEqual(summary.byKind.personal.notionByType, {
    main_event: 4,
    rehearsal: 1,
  });
  assert.deepEqual(summary.byKind.personal.postgresByType, {
    main_event: 4,
    rehearsal: 2,
  });
  assert.equal(summary.byKind.personal.unpairedNotion, 1);
  assert.equal(summary.byKind.personal.unpairedPostgres, 2);
  assert.deepEqual(summary.byKind.personal.unpairedNotionByType, { main_event: 1 });
  assert.deepEqual(summary.byKind.personal.unpairedPostgresByType, { rehearsal: 2 });
  assert.deepEqual(summary.byKind.personal.pairsByMethod, { sourceIdentity: 4, title: 2 });
  assert.deepEqual(summary.byKind.personal.fieldMismatchCountsByType, {
    main_event: { title: 4 },
  });
  assert.equal(summary.byKind.admin.notionEvents, 20);
  assert.equal(summary.byKind.admin.unpairedNotion, 4);
});

test('shadow summary classifies unavailable baselines inside the matching feed kind', () => {
  const summary = summarizeCalendarShadowEntries(
    [
      { kind: 'personal', errorCode: 'SHADOW_BASELINE_CACHE_MISSING' },
      { kind: 'personal', errorCode: 'POSTGRES_CALENDAR_FEED_TIMEOUT' },
    ],
    { baselineUnavailableCodes: new Set(['SHADOW_BASELINE_CACHE_MISSING']) }
  );

  assert.equal(summary.baselineUnavailable, 1);
  assert.equal(summary.errors, 1);
  assert.equal(summary.byKind.personal.baselineUnavailable, 1);
  assert.equal(summary.byKind.personal.errors, 1);
});
