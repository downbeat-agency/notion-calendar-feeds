const SIGNAL_NAMES = new Set([
  'projectionSuccess',
  'projectionFailure',
  'staleFallback',
  'revisionMismatch',
  'snapshotRetry',
  'snapshotSuperseded',
  'slowBuild',
  'emptyRegression',
  'coalescedRequest',
  'conditionalHit',
]);

function clean(value, limit = 200) {
  return String(value ?? '').trim().slice(0, limit);
}

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function createCalendarObservability(options = {}) {
  const env = options.env || process.env;
  const now = options.now || (() => new Date());
  const log = options.log || console.log.bind(console);
  const warn = options.warn || console.warn.bind(console);
  const fetchFn = options.fetchFn || fetch;
  const webhookUrl = clean(
    env.CALENDAR_OPS_ALERT_WEBHOOK_URL || env.OPS_ALERT_WEBHOOK_URL,
    2_000
  );
  const alertCooldownMs = positiveNumber(
    env.CALENDAR_ALERT_COOLDOWN_MS,
    5 * 60 * 1000
  );
  const counters = Object.fromEntries([...SIGNAL_NAMES].map((name) => [name, 0]));
  const lastAlertAt = new Map();
  let lastProjectionSuccessAt = null;
  let lastProjectionFailureAt = null;
  let lastProjectionErrorCode = null;
  let lastProbeAt = null;
  let lastProbeLatencyMs = null;
  let lastProbeRevision = null;
  let consecutiveProbeFailures = 0;
  let timer = null;

  const postAlert = async (signal, details) => {
    if (!webhookUrl) return;
    const timestamp = now().getTime();
    const previous = lastAlertAt.get(signal) || 0;
    if (timestamp - previous < alertCooldownMs) return;
    lastAlertAt.set(signal, timestamp);
    try {
      await fetchFn(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          code: `CALENDAR_${signal.replace(/([a-z])([A-Z])/gu, '$1_$2').toUpperCase()}`,
          severity: signal === 'projectionFailure' ? 'error' : 'warning',
          source: 'notion-calendar-feeds',
          occurredAt: now().toISOString(),
          details,
        }),
      });
    } catch (error) {
      warn(`[calendar-monitor] Alert delivery failed: ${clean(error?.message) || 'unknown'}`);
    }
  };

  const record = (signal, details = {}) => {
    if (!SIGNAL_NAMES.has(signal)) return;
    counters[signal] += 1;
    const safeDetails = {
      kind: clean(details.kind, 40) || null,
      errorCode: clean(details.errorCode, 80) || null,
      durationMs: Number.isFinite(Number(details.durationMs)) ? Number(details.durationMs) : null,
    };
    if (signal === 'projectionSuccess') {
      lastProjectionSuccessAt = now().toISOString();
    } else if (signal === 'projectionFailure') {
      lastProjectionFailureAt = now().toISOString();
      lastProjectionErrorCode = safeDetails.errorCode || 'UNKNOWN';
      warn(`[calendar-monitor] projectionFailure ${JSON.stringify(safeDetails)}`);
      void postAlert(signal, safeDetails);
    } else if (['staleFallback', 'snapshotSuperseded', 'slowBuild', 'emptyRegression'].includes(signal)) {
      warn(`[calendar-monitor] ${signal} ${JSON.stringify(safeDetails)}`);
      void postAlert(signal, safeDetails);
    } else {
      log(`[calendar-monitor] ${signal} ${JSON.stringify(safeDetails)}`);
    }
  };

  const probe = async (probeFn) => {
    const started = Date.now();
    lastProbeAt = now().toISOString();
    try {
      const result = await probeFn();
      lastProbeLatencyMs = Date.now() - started;
      lastProbeRevision = clean(result?.sourceRevision, 100) || null;
      consecutiveProbeFailures = 0;
      record('projectionSuccess', { kind: 'version-probe', durationMs: lastProbeLatencyMs });
      return true;
    } catch (error) {
      lastProbeLatencyMs = Date.now() - started;
      consecutiveProbeFailures += 1;
      record('projectionFailure', {
        kind: 'version-probe',
        errorCode: error?.code || 'CALENDAR_VERSION_PROBE_FAILED',
        durationMs: lastProbeLatencyMs,
      });
      return false;
    }
  };

  const start = (probeFn) => {
    if (typeof probeFn !== 'function' || timer) return timer;
    const intervalMs = positiveNumber(env.CALENDAR_HEALTH_PROBE_INTERVAL_MS, 5 * 60 * 1000);
    void probe(probeFn);
    timer = setInterval(() => void probe(probeFn), intervalMs);
    timer.unref?.();
    return timer;
  };

  const stop = () => {
    if (timer) clearInterval(timer);
    timer = null;
  };

  const snapshot = () => ({
    generatedAt: now().toISOString(),
    counters: { ...counters },
    lastProjectionSuccessAt,
    lastProjectionFailureAt,
    lastProjectionErrorCode,
    probe: {
      lastProbeAt,
      lastProbeLatencyMs,
      lastProbeRevision,
      consecutiveFailures: consecutiveProbeFailures,
    },
  });

  return { probe, record, snapshot, start, stop };
}
