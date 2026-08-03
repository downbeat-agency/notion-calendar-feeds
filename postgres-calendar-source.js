import { createHash, timingSafeEqual } from 'node:crypto';

const VALID_SOURCES = new Set(['notion', 'shadow', 'postgres']);
const DEFAULT_TIMEOUT_MS = 25_000;
const MAX_WEAK_PAIR_DISTANCE_MS = 72 * 60 * 60 * 1000;
const DEFAULT_HISTORY_CUTOVER_DATE = '2026-08-02';

function clean(value, limit = 2_000) {
  return String(value ?? '').trim().slice(0, limit);
}

export function configuredCalendarFeedSource(env = process.env) {
  const source = clean(env.CALENDAR_FEED_SOURCE, 30).toLowerCase() || 'notion';
  if (!VALID_SOURCES.has(source)) {
    throw new Error('CALENDAR_FEED_SOURCE must be notion, shadow, or postgres.');
  }
  return source;
}

export function configuredCalendarHistoryCutoverDate(env = process.env) {
  const value = clean(
    env.CALENDAR_FEED_HISTORY_CUTOVER_DATE || DEFAULT_HISTORY_CUTOVER_DATE,
    10
  );
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(value)) {
    throw new Error('CALENDAR_FEED_HISTORY_CUTOVER_DATE must be YYYY-MM-DD.');
  }
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) {
    throw new Error('CALENDAR_FEED_HISTORY_CUTOVER_DATE must be a real calendar date.');
  }
  return value;
}

function calendarEventDate(event = {}) {
  const direct = clean(event.start, 100).match(/^(\d{4}-\d{2}-\d{2})/u)?.[1];
  if (direct) return direct;
  const parsed = event.start instanceof Date ? event.start : new Date(event.start);
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString().slice(0, 10) : '';
}

export function mergeCalendarEventsAcrossHistoryCutover(
  legacyEvents = [],
  postgresEvents = [],
  cutoverDate = DEFAULT_HISTORY_CUTOVER_DATE
) {
  const boundary = configuredCalendarHistoryCutoverDate({
    CALENDAR_FEED_HISTORY_CUTOVER_DATE: cutoverDate,
  });
  const historical = (Array.isArray(legacyEvents) ? legacyEvents : [])
    .filter((event) => {
      const date = calendarEventDate(event);
      return date && date < boundary;
    });
  const current = (Array.isArray(postgresEvents) ? postgresEvents : [])
    .filter((event) => {
      const date = calendarEventDate(event);
      return !date || date >= boundary;
    });
  return [...historical, ...current];
}

function sourceConfig(env = process.env) {
  const baseUrl = clean(env.CALENDAR_FEED_API_BASE_URL).replace(/\/+$/u, '');
  const serviceKey = clean(env.CALENDAR_FEED_SERVICE_KEY);
  if (!baseUrl) throw new Error('CALENDAR_FEED_API_BASE_URL is required for Postgres calendar reads.');
  if (!serviceKey) throw new Error('CALENDAR_FEED_SERVICE_KEY is required for Postgres calendar reads.');
  return { baseUrl, serviceKey };
}

function feedPath(kind, selector) {
  if (kind === 'personal') {
    const personId = clean(selector, 100);
    if (!personId) throw new Error('A person selector is required for a personal calendar feed.');
    return `/api/internal/calendar-feeds/personal/${encodeURIComponent(personId)}`;
  }
  if (kind === 'version') return '/api/internal/calendar-feeds/version';
  if (kind === 'people') return '/api/internal/calendar-feeds/people';
  if (kind === 'admin' || kind === 'travel' || kind === 'blockout') {
    return `/api/internal/calendar-feeds/${kind}`;
  }
  throw new Error(`Unsupported Postgres calendar feed kind: ${kind}`);
}

export function calendarFeedServiceRequestIsAuthorized(req, env = process.env) {
  const expected = clean(env.CALENDAR_FEED_SERVICE_KEY);
  const supplied = clean(req?.get?.('X-Downbeat-Calendar-Service-Key'));
  if (!expected || !supplied) return false;
  const expectedBuffer = Buffer.from(expected);
  const suppliedBuffer = Buffer.from(supplied);
  return expectedBuffer.length === suppliedBuffer.length
    && timingSafeEqual(expectedBuffer, suppliedBuffer);
}

export async function fetchPostgresCalendarFeed(kind, selector = null, options = {}) {
  const env = options.env || process.env;
  const fetchFn = options.fetchFn || fetch;
  const configuredTimeoutMs = Number(
    options.timeoutMs || env.CALENDAR_FEED_API_TIMEOUT_MS || DEFAULT_TIMEOUT_MS
  );
  const timeoutMs = Number.isFinite(configuredTimeoutMs) && configuredTimeoutMs > 0
    ? configuredTimeoutMs
    : DEFAULT_TIMEOUT_MS;
  const { baseUrl, serviceKey } = sourceConfig(env);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1, timeoutMs));
  try {
    const response = await fetchFn(`${baseUrl}${feedPath(kind, selector)}`, {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        'X-Downbeat-Calendar-Service-Key': serviceKey,
      },
      signal: controller.signal,
    });
    const body = await response.json().catch(() => null);
    if (!response.ok) {
      const error = new Error(
        body?.error || `Postgres calendar feed returned HTTP ${response.status}.`
      );
      error.status = response.status;
      error.code = body?.code || 'POSTGRES_CALENDAR_FEED_HTTP_ERROR';
      throw error;
    }
    if (!body || body.source !== 'postgres' || Number(body.schemaVersion) !== 1) {
      const error = new Error('Postgres calendar feed returned an unsupported payload.');
      error.code = 'POSTGRES_CALENDAR_FEED_SCHEMA_INVALID';
      throw error;
    }
    return body;
  } catch (error) {
    if (error?.name === 'AbortError') {
      const timeoutError = new Error(`Postgres calendar feed timed out after ${timeoutMs}ms.`);
      timeoutError.code = 'POSTGRES_CALENDAR_FEED_TIMEOUT';
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function iso(value) {
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : clean(value);
}

function eventTypeCounts(events = []) {
  return events.reduce((counts, event) => {
    const type = clean(event?.type, 100) || 'unknown';
    counts[type] = (counts[type] || 0) + 1;
    return counts;
  }, {});
}

function eventFingerprint(event = {}) {
  return createHash('sha256').update(JSON.stringify({
    type: clean(event.type),
    title: clean(event.title),
    start: iso(event.start),
    end: iso(event.end),
    description: clean(event.description),
    location: clean(event.location),
    url: clean(event.url),
  })).digest('hex').slice(0, 20);
}

const COMPARED_EVENT_FIELDS = [
  'type',
  'title',
  'start',
  'end',
  'description',
  'location',
  'url',
  'pay',
];

function comparablePay(event = {}) {
  if (clean(event.type, 100) !== 'main_event') return '';
  const match = clean(event.description).match(
    /(?:^|\n)Total Pay:\s*\$?\s*(-?[\d,]+(?:\.\d+)?)/iu
  );
  if (!match) return '';
  const amount = Number(match[1].replaceAll(',', ''));
  return Number.isFinite(amount) ? amount.toFixed(2) : '';
}

function comparableField(event, field) {
  if (field === 'pay') return comparablePay(event);
  return field === 'start' || field === 'end' ? iso(event?.[field]) : clean(event?.[field]);
}

function normalizedIdentity(value) {
  return clean(value)
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim();
}

function canonicalNotionPageId(value) {
  const text = clean(value).toLowerCase();
  const dashed = text.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gu);
  if (dashed?.length) return dashed.at(-1).replaceAll('-', '');
  const compact = text.match(/[0-9a-f]{32}/gu);
  return compact?.at(-1) || '';
}

function canonicalNotionUrlIdentity(value) {
  try {
    const parsed = new URL(clean(value));
    const notionHost = /(?:^|\.)notion\.(?:so|com)$/u.test(parsed.hostname.toLowerCase());
    // A Notion URL can also carry a database-view id in its query string.
    // Only the path identifies the linked page.
    const notionPageId = notionHost ? canonicalNotionPageId(parsed.pathname) : '';
    return notionPageId ? `notion:${notionPageId}` : '';
  } catch {
    return '';
  }
}

function canonicalDownbeatTimelineIdentity(value) {
  const candidates = clean(value).match(/https?:\/\/[^\s<>]+/giu) || [];
  for (const candidate of candidates) {
    try {
      const parsed = new URL(candidate);
      if (parsed.hostname.toLowerCase() !== 'music.downbeat.agency') continue;
      const match = parsed.pathname.match(
        /^\/timeline\/(?:[eq]\/)?([0-9a-f]{32}|[0-9a-f-]{36})(?:\/|$)/iu
      );
      if (match) return `event:${match[1].toLowerCase().replaceAll('-', '')}`;
    } catch {
      // Ignore a malformed candidate and continue looking for a valid timeline link.
    }
  }
  return '';
}

function mainEventComparisonIdentity(event = {}) {
  if (clean(event.type, 100) !== 'main_event') return '';
  const explicit = clean(event.comparisonIdentity, 200).toLowerCase();
  if (/^event:[0-9a-f]{32}$/u.test(explicit)) return explicit;
  const timelineIdentity = canonicalDownbeatTimelineIdentity(event.description);
  if (timelineIdentity) return timelineIdentity;
  const notionIdentity = canonicalNotionUrlIdentity(event.url)
    || embeddedNotionUrlIdentity(event.description);
  return notionIdentity ? notionIdentity.replace(/^notion:/u, 'event:') : '';
}

function canonicalUrlIdentity(value) {
  const text = clean(value);
  if (!text) return '';
  const notionIdentity = canonicalNotionUrlIdentity(text);
  if (notionIdentity) return notionIdentity;
  try {
    const parsed = new URL(text);
    parsed.hash = '';
    if (parsed.pathname !== '/') parsed.pathname = parsed.pathname.replace(/\/+$/u, '');
    return parsed.toString();
  } catch {
    return normalizedIdentity(text);
  }
}

function embeddedNotionUrlIdentity(value) {
  const candidates = clean(value).match(/https?:\/\/[^\s<>]+/giu) || [];
  for (const candidate of candidates) {
    const identity = canonicalNotionUrlIdentity(candidate);
    if (identity) return identity;
  }
  return '';
}

function normalizeEmbeddedNotionUrls(value) {
  return clean(value).replace(/https?:\/\/[^\s<>]+/giu, (candidate) => {
    const notionIdentity = canonicalNotionUrlIdentity(candidate);
    return notionIdentity ? ` ${notionIdentity} ` : candidate;
  });
}

function semanticallyComparableField(event, field) {
  if (field === 'pay') return comparablePay(event);
  if (field === 'start' || field === 'end') return iso(event?.[field]);
  if (field === 'type') return normalizedIdentity(event?.[field]);
  if (field === 'url') return canonicalUrlIdentity(event?.[field]);
  return normalizedIdentity(normalizeEmbeddedNotionUrls(event?.[field]));
}

function baseTitle(event = {}) {
  return normalizedIdentity(clean(event.title).replace(/\s*\([^)]*\)\s*$/u, ''));
}

function groupUnmatched(entries, keyFn) {
  const groups = new Map();
  for (const entry of entries) {
    if (entry.matched) continue;
    const key = keyFn(entry.event);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  return groups;
}

function eventStartDistance(left, right) {
  const leftMs = new Date(left?.start).getTime();
  const rightMs = new Date(right?.start).getTime();
  if (!Number.isFinite(leftMs) || !Number.isFinite(rightMs)) return Number.POSITIVE_INFINITY;
  return Math.abs(leftMs - rightMs);
}

function pairByKey(
  notionEntries,
  postgresEntries,
  keyFn,
  method,
  pairs,
  pairsByMethod,
  maxDistanceMs = Number.POSITIVE_INFINITY
) {
  const notionGroups = groupUnmatched(notionEntries, keyFn);
  const postgresGroups = groupUnmatched(postgresEntries, keyFn);
  for (const [key, notionGroup] of notionGroups) {
    const postgresGroup = postgresGroups.get(key);
    if (!postgresGroup) continue;
    notionGroup.sort((left, right) => comparableField(left.event, 'start')
      .localeCompare(comparableField(right.event, 'start')));
    for (const notionEntry of notionGroup) {
      const match = postgresGroup
        .filter((postgresEntry) => !postgresEntry.matched
          && eventStartDistance(notionEntry.event, postgresEntry.event) <= maxDistanceMs)
        .sort((left, right) => eventStartDistance(notionEntry.event, left.event)
          - eventStartDistance(notionEntry.event, right.event))[0];
      if (!match) break;
      notionEntry.matched = true;
      match.matched = true;
      pairs.push([notionEntry.event, match.event, method]);
      pairsByMethod[method] = (pairsByMethod[method] || 0) + 1;
    }
  }
}

function pairByContainedIdentity(
  notionEntries,
  postgresEntries,
  identityFn,
  method,
  pairs,
  pairsByMethod,
  maxDistanceMs = Number.POSITIVE_INFINITY
) {
  for (const notionEntry of notionEntries) {
    if (notionEntry.matched) continue;
    const notionType = comparableField(notionEntry.event, 'type');
    const notionIdentity = identityFn(notionEntry.event);
    if (notionIdentity.length < 8) continue;
    const matches = postgresEntries.filter((postgresEntry) => {
      if (
        postgresEntry.matched
        || comparableField(postgresEntry.event, 'type') !== notionType
        || eventStartDistance(notionEntry.event, postgresEntry.event) > maxDistanceMs
      ) return false;
      const postgresIdentity = identityFn(postgresEntry.event);
      return postgresIdentity.length >= 8
        && (notionIdentity.includes(postgresIdentity) || postgresIdentity.includes(notionIdentity));
    });
    const match = matches.sort((left, right) => eventStartDistance(notionEntry.event, left.event)
      - eventStartDistance(notionEntry.event, right.event))[0];
    if (!match) continue;
    notionEntry.matched = true;
    match.matched = true;
    pairs.push([notionEntry.event, match.event, method]);
    pairsByMethod[method] = (pairsByMethod[method] || 0) + 1;
  }
}

export function pairCalendarEvents(notionEvents, postgresEvents) {
  const notionEntries = notionEvents.map((event) => ({ event, matched: false }));
  const postgresEntries = postgresEvents.map((event) => ({ event, matched: false }));
  const pairs = [];
  const pairsByMethod = {};
  const typed = (valueFn) => (event) => {
    const value = valueFn(event);
    return value ? `${comparableField(event, 'type')}|${value}` : '';
  };

  pairByKey(
    notionEntries,
    postgresEntries,
    typed(mainEventComparisonIdentity),
    'sourceIdentity',
    pairs,
    pairsByMethod
  );
  pairByKey(notionEntries, postgresEntries, typed((event) => canonicalUrlIdentity(event.url)), 'url', pairs, pairsByMethod);
  pairByKey(
    notionEntries,
    postgresEntries,
    typed((event) => embeddedNotionUrlIdentity(event.description)),
    'descriptionUrl',
    pairs,
    pairsByMethod
  );
  pairByKey(
    notionEntries,
    postgresEntries,
    typed((event) => normalizedIdentity(event.mainEvent)),
    'mainEvent',
    pairs,
    pairsByMethod,
    MAX_WEAK_PAIR_DISTANCE_MS
  );
  pairByKey(
    notionEntries,
    postgresEntries,
    typed(baseTitle),
    'title',
    pairs,
    pairsByMethod,
    MAX_WEAK_PAIR_DISTANCE_MS
  );
  pairByContainedIdentity(
    notionEntries,
    postgresEntries,
    (event) => normalizedIdentity(event.mainEvent),
    'containedMainEvent',
    pairs,
    pairsByMethod,
    MAX_WEAK_PAIR_DISTANCE_MS
  );
  pairByContainedIdentity(
    notionEntries,
    postgresEntries,
    baseTitle,
    'containedTitle',
    pairs,
    pairsByMethod,
    MAX_WEAK_PAIR_DISTANCE_MS
  );
  pairByKey(notionEntries, postgresEntries, typed((event) => comparableField(event, 'start')), 'start', pairs, pairsByMethod);

  const unpairedNotion = notionEntries
    .filter((entry) => !entry.matched)
    .map((entry) => entry.event);
  const unpairedPostgres = postgresEntries
    .filter((entry) => !entry.matched)
    .map((entry) => entry.event);
  return {
    pairs,
    pairsByMethod,
    unpairedNotion,
    unpairedPostgres,
    unpairedNotionCount: unpairedNotion.length,
    unpairedPostgresCount: unpairedPostgres.length,
    unpairedNotionByType: eventTypeCounts(unpairedNotion),
    unpairedPostgresByType: eventTypeCounts(unpairedPostgres),
  };
}

function diagnosticEvent(event = {}) {
  const type = clean(event.type, 100) || 'unknown';
  const sourceIdentity = mainEventComparisonIdentity(event)
    || canonicalNotionUrlIdentity(event.url)
    || embeddedNotionUrlIdentity(event.description)
    || null;
  return {
    type,
    start: iso(event.start),
    end: iso(event.end),
    sourceIdentity,
    fingerprint: eventFingerprint(event),
  };
}

export function diagnoseCalendarEventSets(notionEvents = [], postgresEvents = []) {
  const paired = pairCalendarEvents(notionEvents, postgresEvents);
  const pairedFieldDrift = paired.pairs.flatMap(([notionEvent, postgresEvent, method]) => {
    const exactFields = COMPARED_EVENT_FIELDS.filter((field) =>
      comparableField(notionEvent, field) !== comparableField(postgresEvent, field)
    );
    const semanticFields = COMPARED_EVENT_FIELDS.filter((field) =>
      semanticallyComparableField(notionEvent, field)
        !== semanticallyComparableField(postgresEvent, field)
    );
    if (exactFields.length === 0) return [];
    return [{
      method,
      notion: diagnosticEvent(notionEvent),
      postgres: diagnosticEvent(postgresEvent),
      exactFields,
      semanticFields,
    }];
  });
  return {
    unpairedNotion: paired.unpairedNotion.map(diagnosticEvent),
    unpairedPostgres: paired.unpairedPostgres.map(diagnosticEvent),
    pairedFieldDrift,
  };
}

function fingerprintDifference(leftEvents, rightEvents) {
  const rightCounts = new Map();
  for (const event of rightEvents) {
    const fingerprint = eventFingerprint(event);
    rightCounts.set(fingerprint, (rightCounts.get(fingerprint) || 0) + 1);
  }
  const difference = [];
  for (const event of leftEvents) {
    const fingerprint = eventFingerprint(event);
    const remaining = rightCounts.get(fingerprint) || 0;
    if (remaining > 0) rightCounts.set(fingerprint, remaining - 1);
    else difference.push(fingerprint);
  }
  return difference;
}

export function compareCalendarEventSets(notionEvents = [], postgresEvents = []) {
  const missingFromPostgres = fingerprintDifference(notionEvents, postgresEvents);
  const extraInPostgres = fingerprintDifference(postgresEvents, notionEvents);
  const paired = pairCalendarEvents(notionEvents, postgresEvents);
  const fieldMismatchCounts = Object.fromEntries(COMPARED_EVENT_FIELDS.map((field) => [field, 0]));
  const semanticFieldMismatchCounts = Object.fromEntries(
    COMPARED_EVENT_FIELDS.map((field) => [field, 0])
  );
  const fieldMismatchCountsByType = {};
  const semanticFieldMismatchCountsByType = {};
  let exactPairCount = 0;
  let semanticPairCount = 0;
  for (const [notionEvent, postgresEvent] of paired.pairs) {
    const type = clean(notionEvent?.type, 100) || clean(postgresEvent?.type, 100) || 'unknown';
    const byType = fieldMismatchCountsByType[type] || Object.fromEntries(
      COMPARED_EVENT_FIELDS.map((field) => [field, 0])
    );
    const semanticByType = semanticFieldMismatchCountsByType[type] || Object.fromEntries(
      COMPARED_EVENT_FIELDS.map((field) => [field, 0])
    );
    let exactPair = true;
    let semanticPair = true;
    for (const field of COMPARED_EVENT_FIELDS) {
      if (comparableField(notionEvent, field) !== comparableField(postgresEvent, field)) {
        fieldMismatchCounts[field] += 1;
        byType[field] += 1;
        exactPair = false;
      }
      if (
        semanticallyComparableField(notionEvent, field)
        !== semanticallyComparableField(postgresEvent, field)
      ) {
        semanticFieldMismatchCounts[field] += 1;
        semanticByType[field] += 1;
        semanticPair = false;
      }
    }
    fieldMismatchCountsByType[type] = byType;
    semanticFieldMismatchCountsByType[type] = semanticByType;
    if (exactPair) exactPairCount += 1;
    if (semanticPair) semanticPairCount += 1;
  }
  const semanticMismatchTotal = Object.values(semanticFieldMismatchCounts)
    .reduce((sum, count) => sum + count, 0);
  return {
    matches: missingFromPostgres.length === 0 && extraInPostgres.length === 0,
    semanticMatches: paired.unpairedNotionCount === 0
      && paired.unpairedPostgresCount === 0
      && semanticMismatchTotal === 0,
    notionCount: notionEvents.length,
    postgresCount: postgresEvents.length,
    notionByType: eventTypeCounts(notionEvents),
    postgresByType: eventTypeCounts(postgresEvents),
    missingFromPostgresCount: missingFromPostgres.length,
    extraInPostgresCount: extraInPostgres.length,
    pairedCount: paired.pairs.length,
    pairsByMethod: paired.pairsByMethod,
    unpairedNotionCount: paired.unpairedNotionCount,
    unpairedPostgresCount: paired.unpairedPostgresCount,
    unpairedNotionByType: paired.unpairedNotionByType,
    unpairedPostgresByType: paired.unpairedPostgresByType,
    exactPairCount,
    semanticPairCount,
    fieldMismatchCounts,
    semanticFieldMismatchCounts,
    fieldMismatchCountsByType,
    semanticFieldMismatchCountsByType,
    // Hashes are safe diagnostics: no calendar titles, locations, names, or notes are logged.
    missingFingerprintSample: missingFromPostgres.slice(0, 10),
    extraFingerprintSample: extraInPostgres.slice(0, 10),
  };
}
