import { createHash } from 'node:crypto';

function unstableSnapshotError(cause = null) {
  const error = new Error('Notion calendar formula snapshot did not stabilize.');
  error.code = 'NOTION_CALENDAR_FORMULA_UNSTABLE';
  if (cause) error.cause = cause;
  return error;
}

function signature(value) {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

export async function readStableFormulaSnapshot(readOnce, options = {}) {
  if (typeof readOnce !== 'function') throw new TypeError('readOnce must be a function');
  const attempts = Math.max(1, Math.min(Number(options.attempts) || 3, 6));
  const requiredHits = Math.max(
    1,
    Math.min(Number(options.requiredHits) || 2, attempts)
  );
  const scoreSnapshot = typeof options.scoreSnapshot === 'function'
    ? options.scoreSnapshot
    : () => 0;
  const pause = typeof options.pause === 'function' ? options.pause : async () => {};
  const parallel = options.parallel === true;
  const observations = new Map();
  let maximumObservedScore = Number.NEGATIVE_INFINITY;
  let lastError = null;

  const observeOnce = async () => {
    try {
      const snapshot = await readOnce();
      const score = Number(scoreSnapshot(snapshot));
      if (!Number.isFinite(score) || score < 0) throw new Error('invalid snapshot score');
      const key = signature(snapshot);
      const observation = observations.get(key) || { snapshot, score, hits: 0 };
      observation.hits += 1;
      observations.set(key, observation);
      maximumObservedScore = Math.max(maximumObservedScore, score);
    } catch (error) {
      lastError = error;
    }
  };

  if (parallel) {
    await Promise.all(Array.from({ length: attempts }, () => observeOnce()));
  } else {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      await observeOnce();
      if (attempt + 1 < attempts) await pause();
    }
  }

  const stable = [...observations.values()]
    .filter((observation) => observation.hits >= requiredHits)
    .filter((observation) => observation.score === maximumObservedScore)
    .sort((left, right) => right.hits - left.hits)[0];
  if (!stable) throw unstableSnapshotError(lastError);
  return stable.snapshot;
}
