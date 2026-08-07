import test from 'node:test';
import assert from 'node:assert/strict';

import { createPostgresCalendarRefreshWorker } from './calendar-refresh-worker.js';

const jobs = [
  {
    id: '11111111-1111-4111-8111-111111111111',
    feedKind: 'personal',
    selector: '22222222-2222-4222-8222-222222222222',
    generation: 2,
    attemptCount: 1,
  },
  {
    id: '33333333-3333-4333-8333-333333333333',
    feedKind: 'admin',
    selector: '',
    generation: 4,
    attemptCount: 3,
  },
];

test('refresh worker completes successful jobs and records failed jobs', async () => {
  const completed = [];
  const failed = [];
  const worker = createPostgresCalendarRefreshWorker({
    env: {},
    workerId: 'edge-test',
    claimFn: async () => jobs,
    regenerateJob: async (job) => (
      job.feedKind === 'personal'
        ? { success: true, sourceRevision: '942100' }
        : { success: false, reason: 'ADMIN_REFRESH_FAILED' }
    ),
    completeFn: async (id, input) => completed.push({ id, input }),
    failFn: async (id, input) => failed.push({ id, input }),
    logger: { warn() {} },
  });

  assert.equal(await worker.poll(), 2);
  assert.deepEqual(completed, [{
    id: jobs[0].id,
    input: {
      workerId: 'edge-test',
      generation: 2,
      sourceRevision: '942100',
    },
  }]);
  assert.deepEqual(failed, [{
    id: jobs[1].id,
    input: {
      workerId: 'edge-test',
      generation: 4,
      attemptCount: 3,
      errorCode: 'ADMIN_REFRESH_FAILED',
    },
  }]);
  assert.equal(worker.snapshot().completed, 1);
  assert.equal(worker.snapshot().failed, 1);
});

test('refresh worker can be disabled without scheduling work', () => {
  const worker = createPostgresCalendarRefreshWorker({
    env: { CALENDAR_REFRESH_WORKER_ENABLED: 'false' },
    regenerateJob: async () => ({ success: true }),
  });
  assert.equal(worker.start(), false);
  assert.equal(worker.snapshot().started, false);
});
