// Version 3 also requires an independent Calendar Data occurrence-set witness,
// so a repeatedly false-empty formula render cannot satisfy a new audit.
const SHADOW_BASELINE_SCHEMA_VERSION = 3;
const VALID_BASELINE_KINDS = new Set(['personal', 'admin', 'travel', 'blockout']);

function baselineError(message, code) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function cleanPart(value, label) {
  const cleaned = String(value || '').trim();
  if (!cleaned) throw baselineError(`${label} is required.`, 'SHADOW_BASELINE_SELECTOR_INVALID');
  return cleaned;
}

export function calendarShadowBaselineKey(kind, selector) {
  const cleanedKind = cleanPart(kind, 'Baseline kind').toLowerCase();
  if (!VALID_BASELINE_KINDS.has(cleanedKind)) {
    throw baselineError('Unsupported calendar shadow baseline kind.', 'SHADOW_BASELINE_KIND_INVALID');
  }
  const cleanedSelector = cleanPart(selector, 'Baseline selector').toLowerCase();
  return `calendar:shadow:notion-baseline:v${SHADOW_BASELINE_SCHEMA_VERSION}:${cleanedKind}:${cleanedSelector}`;
}

export async function persistCalendarShadowBaseline(client, options = {}) {
  if (!client?.set) {
    throw baselineError('Calendar shadow baseline cache is unavailable.', 'SHADOW_AUDIT_CACHE_UNAVAILABLE');
  }
  if (!Array.isArray(options.events)) {
    throw baselineError('Calendar shadow baseline events must be an array.', 'SHADOW_BASELINE_CACHE_INVALID');
  }

  const kind = cleanPart(options.kind, 'Baseline kind').toLowerCase();
  const selector = cleanPart(options.selector, 'Baseline selector').toLowerCase();
  const capturedAt = options.capturedAt || new Date().toISOString();
  const payload = {
    schemaVersion: SHADOW_BASELINE_SCHEMA_VERSION,
    kind,
    selector,
    capturedAt,
    sourcePageId: options.sourcePageId || null,
    events: options.events,
  };

  // Deliberately no expiration: a full-fleet audit can take longer than the
  // public delivery cache TTL. A later successful Notion refresh replaces it.
  await client.set(calendarShadowBaselineKey(kind, selector), JSON.stringify(payload));
  return payload;
}

export async function loadCalendarShadowBaseline(client, kind, selector) {
  if (!client?.get) {
    throw baselineError('Calendar shadow baseline cache is unavailable.', 'SHADOW_AUDIT_CACHE_UNAVAILABLE');
  }
  const raw = await client.get(calendarShadowBaselineKey(kind, selector));
  if (!raw) {
    throw baselineError('Calendar shadow baseline cache is missing.', 'SHADOW_BASELINE_CACHE_MISSING');
  }
  try {
    const payload = JSON.parse(raw);
    if (
      Number(payload?.schemaVersion) !== SHADOW_BASELINE_SCHEMA_VERSION
      || payload?.kind !== String(kind || '').trim().toLowerCase()
      || String(payload?.selector || '') !== String(selector || '').trim().toLowerCase()
      || !Array.isArray(payload?.events)
    ) {
      throw new Error('unsupported baseline payload');
    }
    return payload;
  } catch {
    throw baselineError('Calendar shadow baseline cache is invalid.', 'SHADOW_BASELINE_CACHE_INVALID');
  }
}
