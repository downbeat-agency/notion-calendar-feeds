import { createHash, timingSafeEqual } from 'node:crypto';

const VALID_SOURCES = new Set(['notion', 'shadow', 'postgres']);
const DEFAULT_TIMEOUT_MS = 25_000;

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

const COMPARED_EVENT_FIELDS = ['type', 'title', 'start', 'end', 'description', 'location', 'url'];

function comparableField(event, field) {
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
    const notionPageId = notionHost ? canonicalNotionPageId(parsed.href) : '';
    return notionPageId ? `notion:${notionPageId}` : '';
  } catch {
    return '';
  }
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

function normalizeEmbeddedNotionUrls(value) {
  return clean(value).replace(/https?:\/\/[^\s<>]+/giu, (candidate) => {
    const notionIdentity = canonicalNotionUrlIdentity(candidate);
    return notionIdentity ? ` ${notionIdentity} ` : candidate;
  });
}

function semanticallyComparableField(event, field) {
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

function pairByKey(notionEntries, postgresEntries, keyFn, method, pairs, pairsByMethod) {
  const notionGroups = groupUnmatched(notionEntries, keyFn);
  const postgresGroups = groupUnmatched(postgresEntries, keyFn);
  for (const [key, notionGroup] of notionGroups) {
    const postgresGroup = postgresGroups.get(key);
    if (!postgresGroup) continue;
    notionGroup.sort((left, right) => comparableField(left.event, 'start')
      .localeCompare(comparableField(right.event, 'start')));
    for (const notionEntry of notionGroup) {
      const match = postgresGroup
        .filter((postgresEntry) => !postgresEntry.matched)
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
  pairsByMethod
) {
  for (const notionEntry of notionEntries) {
    if (notionEntry.matched) continue;
    const notionType = comparableField(notionEntry.event, 'type');
    const notionIdentity = identityFn(notionEntry.event);
    if (notionIdentity.length < 8) continue;
    const matches = postgresEntries.filter((postgresEntry) => {
      if (postgresEntry.matched || comparableField(postgresEntry.event, 'type') !== notionType) return false;
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

  pairByKey(notionEntries, postgresEntries, typed((event) => canonicalUrlIdentity(event.url)), 'url', pairs, pairsByMethod);
  pairByKey(notionEntries, postgresEntries, typed((event) => normalizedIdentity(event.mainEvent)), 'mainEvent', pairs, pairsByMethod);
  pairByKey(notionEntries, postgresEntries, typed(baseTitle), 'title', pairs, pairsByMethod);
  pairByContainedIdentity(notionEntries, postgresEntries, (event) => normalizedIdentity(event.mainEvent), 'containedMainEvent', pairs, pairsByMethod);
  pairByContainedIdentity(notionEntries, postgresEntries, baseTitle, 'containedTitle', pairs, pairsByMethod);
  pairByKey(notionEntries, postgresEntries, typed((event) => comparableField(event, 'start')), 'start', pairs, pairsByMethod);

  return {
    pairs,
    pairsByMethod,
    unpairedNotionCount: notionEntries.filter((entry) => !entry.matched).length,
    unpairedPostgresCount: postgresEntries.filter((entry) => !entry.matched).length,
    unpairedNotionByType: eventTypeCounts(
      notionEntries.filter((entry) => !entry.matched).map((entry) => entry.event)
    ),
    unpairedPostgresByType: eventTypeCounts(
      postgresEntries.filter((entry) => !entry.matched).map((entry) => entry.event)
    ),
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
