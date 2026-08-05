import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildCalendarArtifactMetadata,
  calendarArtifactMetadataMatches,
  calendarRequestIsNotModified,
  createCalendarSingleFlight,
  parseCalendarArtifactMetadata,
  resolveCalendarRendererVersion,
} from './calendar-cache-policy.js';

test('renderer version changes with deployments and time policy', () => {
  const first = resolveCalendarRendererVersion({
    RAILWAY_GIT_COMMIT_SHA: 'abc',
    CALENDAR_TIME_MODE: 'floating',
  });
  assert.equal(first, resolveCalendarRendererVersion({
    RAILWAY_GIT_COMMIT_SHA: 'abc',
    CALENDAR_TIME_MODE: 'floating',
  }));
  assert.notEqual(first, resolveCalendarRendererVersion({
    RAILWAY_GIT_COMMIT_SHA: 'def',
    CALENDAR_TIME_MODE: 'floating',
  }));
  assert.notEqual(first, resolveCalendarRendererVersion({
    RAILWAY_GIT_COMMIT_SHA: 'abc',
    CALENDAR_TIME_MODE: 'legacy-la',
  }));
});

test('artifact metadata binds one format to source and renderer revisions', () => {
  const metadata = buildCalendarArtifactMetadata({
    content: 'BEGIN:VCALENDAR',
    sourceRevision: '42',
    rendererVersion: 'renderer-a',
    generatedAt: '2026-08-05T12:00:00.000Z',
    eventCount: 3,
  });
  assert.equal(
    calendarArtifactMetadataMatches(metadata, {
      sourceRevision: '42',
      rendererVersion: 'renderer-a',
    }),
    true
  );
  assert.equal(
    calendarArtifactMetadataMatches(metadata, {
      sourceRevision: '43',
      rendererVersion: 'renderer-a',
    }),
    false
  );
  assert.equal(parseCalendarArtifactMetadata(JSON.stringify(metadata))?.eventCount, 3);
});

test('HTTP validators recognize unchanged calendar artifacts', () => {
  const metadata = buildCalendarArtifactMetadata({
    content: 'calendar',
    rendererVersion: 'renderer-a',
    generatedAt: '2026-08-05T12:00:00.000Z',
  });
  assert.equal(calendarRequestIsNotModified({ 'if-none-match': metadata.etag }, metadata), true);
  assert.equal(calendarRequestIsNotModified({ 'if-none-match': '"different"' }, metadata), false);
  assert.equal(
    calendarRequestIsNotModified({ 'if-modified-since': 'Wed, 05 Aug 2026 12:00:00 GMT' }, metadata),
    true
  );
});

test('single-flight shares one build and cleans up after completion', async () => {
  let builds = 0;
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const run = createCalendarSingleFlight();
  const first = run('personal:1', async () => {
    builds += 1;
    await gate;
    return 'ready';
  });
  const second = run('personal:1', async () => {
    builds += 1;
    return 'duplicate';
  });
  assert.equal(first, second);
  assert.equal(run.activeCount(), 1);
  release();
  assert.deepEqual(await Promise.all([first, second]), ['ready', 'ready']);
  assert.equal(builds, 1);
  assert.equal(run.activeCount(), 0);
});
