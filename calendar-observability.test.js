import assert from 'node:assert/strict';
import test from 'node:test';

import { createCalendarObservability } from './calendar-observability.js';

test('calendar observability records only operational metadata', async () => {
  const monitor = createCalendarObservability({
    env: {},
    log: () => {},
    warn: () => {},
    now: () => new Date('2026-08-05T12:00:00.000Z'),
  });
  monitor.record('staleFallback', { kind: 'personal', personId: 'private-person' });
  await monitor.probe(async () => ({ sourceRevision: '42' }));
  const snapshot = monitor.snapshot();
  assert.equal(snapshot.counters.staleFallback, 1);
  assert.equal(snapshot.counters.projectionSuccess, 1);
  assert.equal(snapshot.probe.lastProbeRevision, '42');
  assert.doesNotMatch(JSON.stringify(snapshot), /private-person/u);
});

test('calendar observability counts failed probes without throwing', async () => {
  const monitor = createCalendarObservability({
    env: {},
    log: () => {},
    warn: () => {},
  });
  assert.equal(await monitor.probe(async () => {
    const error = new Error('down');
    error.code = 'POSTGRES_DOWN';
    throw error;
  }), false);
  const snapshot = monitor.snapshot();
  assert.equal(snapshot.counters.projectionFailure, 1);
  assert.equal(snapshot.probe.consecutiveFailures, 1);
  assert.equal(snapshot.lastProjectionErrorCode, 'POSTGRES_DOWN');
});
