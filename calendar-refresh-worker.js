import { randomBytes } from 'node:crypto';

import {
  claimPostgresCalendarRefreshJobs,
  completePostgresCalendarRefreshJob,
  failPostgresCalendarRefreshJob,
} from './postgres-calendar-source.js';

const DEFAULT_POLL_INTERVAL_MS = 15_000;
const DEFAULT_BATCH_SIZE = 10;
const DEFAULT_CONCURRENCY = 2;

function positiveInteger(value, fallback, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? Math.min(parsed, max) : fallback;
}

function clean(value, limit = 200) {
  return String(value ?? '').trim().slice(0, limit);
}

function enabled(env) {
  return !/^(0|false|no|off)$/iu.test(clean(env.CALENDAR_REFRESH_WORKER_ENABLED, 20));
}

async function mapWithConcurrency(items, concurrency, mapper) {
  let cursor = 0;
  const results = new Array(items.length);
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await mapper(items[index]);
    }
  });
  await Promise.all(workers);
  return results;
}

export function createPostgresCalendarRefreshWorker(options = {}) {
  const env = options.env || process.env;
  const claimFn = options.claimFn || claimPostgresCalendarRefreshJobs;
  const completeFn = options.completeFn || completePostgresCalendarRefreshJob;
  const failFn = options.failFn || failPostgresCalendarRefreshJob;
  const regenerateJob = options.regenerateJob;
  const logger = options.logger || console;
  const pollIntervalMs = positiveInteger(
    env.CALENDAR_REFRESH_POLL_INTERVAL_MS,
    DEFAULT_POLL_INTERVAL_MS,
    5 * 60_000
  );
  const batchSize = positiveInteger(env.CALENDAR_REFRESH_BATCH_SIZE, DEFAULT_BATCH_SIZE, 50);
  const concurrency = positiveInteger(
    env.CALENDAR_REFRESH_CONCURRENCY,
    DEFAULT_CONCURRENCY,
    5
  );
  const workerId = clean(options.workerId, 200)
    || `${process.pid}:${randomBytes(6).toString('hex')}`;

  let started = false;
  let timer = null;
  let pollPromise = null;
  const stats = {
    claimed: 0,
    completed: 0,
    failed: 0,
    lastPollAt: null,
    lastSuccessAt: null,
    lastErrorCode: null,
  };

  async function processJob(job) {
    try {
      const result = await regenerateJob(job);
      if (!result?.success) {
        const error = new Error('Calendar regeneration did not complete.');
        error.code = result?.reason || result?.code || 'CALENDAR_REFRESH_REGEN_FAILED';
        throw error;
      }
      await completeFn(job.id, {
        workerId,
        generation: job.generation,
        sourceRevision: result.sourceRevision || '',
      }, { env });
      stats.completed += 1;
      stats.lastSuccessAt = new Date().toISOString();
      stats.lastErrorCode = null;
      return true;
    } catch (error) {
      const errorCode = clean(error?.code, 120) || 'CALENDAR_REFRESH_REGEN_FAILED';
      stats.failed += 1;
      stats.lastErrorCode = errorCode;
      logger.warn?.(`[calendar-refresh] ${job.feedKind} refresh failed: ${errorCode}`);
      try {
        await failFn(job.id, {
          workerId,
          generation: job.generation,
          attemptCount: job.attemptCount,
          errorCode,
        }, { env });
      } catch (recordError) {
        logger.warn?.(
          `[calendar-refresh] Could not record failed job: ${clean(recordError?.code, 120) || 'UNKNOWN'}`
        );
      }
      return false;
    }
  }

  async function poll() {
    if (pollPromise) return pollPromise;
    pollPromise = (async () => {
      stats.lastPollAt = new Date().toISOString();
      const jobs = await claimFn({ workerId, limit: batchSize }, { env });
      stats.claimed += jobs.length;
      if (jobs.length > 0) {
        await mapWithConcurrency(jobs, concurrency, processJob);
      }
      return jobs.length;
    })();
    try {
      return await pollPromise;
    } finally {
      pollPromise = null;
    }
  }

  function schedule(delayMs = pollIntervalMs) {
    if (!started) return;
    timer = setTimeout(async () => {
      let claimed = 0;
      try {
        claimed = await poll();
      } catch (error) {
        stats.lastErrorCode = clean(error?.code, 120) || 'CALENDAR_REFRESH_POLL_FAILED';
        logger.warn?.(`[calendar-refresh] Poll failed: ${stats.lastErrorCode}`);
      } finally {
        schedule(claimed >= batchSize ? 0 : pollIntervalMs);
      }
    }, Math.max(0, delayMs));
    timer.unref?.();
  }

  function start() {
    if (started || !enabled(env) || typeof regenerateJob !== 'function') return false;
    started = true;
    schedule(0);
    return true;
  }

  function stop() {
    started = false;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  function snapshot() {
    return {
      enabled: enabled(env),
      started,
      workerId,
      pollIntervalMs,
      batchSize,
      concurrency,
      ...stats,
    };
  }

  return { poll, snapshot, start, stop };
}
