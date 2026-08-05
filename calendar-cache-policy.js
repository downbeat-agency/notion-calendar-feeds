import { createHash } from 'node:crypto';

const CALENDAR_RENDERER_SCHEMA_VERSION = 'calendar-renderer-v2';

function clean(value, limit = 500) {
  return String(value ?? '').trim().slice(0, limit);
}

export function resolveCalendarRendererVersion(env = process.env) {
  const deployment = [
    env.CALENDAR_RENDERER_VERSION,
    env.RAILWAY_GIT_COMMIT_SHA,
    env.RENDER_GIT_COMMIT,
    env.SOURCE_VERSION,
    env.GIT_COMMIT_SHA,
  ].map((value) => clean(value, 120)).find(Boolean) || 'local';
  const timeMode = clean(env.CALENDAR_TIME_MODE, 30).toLowerCase() || 'floating';
  return createHash('sha256')
    .update(`${CALENDAR_RENDERER_SCHEMA_VERSION}:${deployment}:${timeMode}`)
    .digest('hex')
    .slice(0, 20);
}

export function calendarArtifactMetadataKey(artifactCacheKey) {
  return `${artifactCacheKey}:metadata`;
}

export function buildCalendarArtifactMetadata({
  content,
  sourceRevision,
  sourceUpdatedAt = null,
  rendererVersion,
  eventCount = null,
  generatedAt = new Date().toISOString(),
} = {}) {
  const body = typeof content === 'string' ? content : JSON.stringify(content ?? '');
  return {
    schemaVersion: 1,
    sourceRevision: sourceRevision == null ? null : String(sourceRevision),
    sourceUpdatedAt: sourceUpdatedAt ? String(sourceUpdatedAt) : null,
    rendererVersion: clean(rendererVersion, 100),
    generatedAt: new Date(generatedAt).toISOString(),
    eventCount: Number.isFinite(Number(eventCount)) ? Number(eventCount) : null,
    etag: `"${createHash('sha256').update(body).digest('base64url')}"`,
  };
}

export function parseCalendarArtifactMetadata(raw) {
  if (!raw) return null;
  try {
    const value = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (
      Number(value?.schemaVersion) !== 1
      || !clean(value?.rendererVersion)
      || !clean(value?.etag)
      || !Number.isFinite(new Date(value?.generatedAt).getTime())
    ) return null;
    return value;
  } catch {
    return null;
  }
}

export function calendarArtifactMetadataMatches(
  metadata,
  { sourceRevision = null, rendererVersion } = {}
) {
  if (!metadata || metadata.rendererVersion !== rendererVersion) return false;
  if (sourceRevision == null || String(sourceRevision) === '') return true;
  return metadata.sourceRevision === String(sourceRevision);
}

function normalizedEtags(value) {
  return clean(value, 5_000)
    .split(',')
    .map((entry) => entry.trim().replace(/^W\//u, ''))
    .filter(Boolean);
}

export function calendarRequestIsNotModified(headers = {}, metadata = null) {
  if (!metadata) return false;
  const readHeader = (name) => headers[name] ?? headers[name.toLowerCase()] ?? '';
  const ifNoneMatch = readHeader('if-none-match');
  if (ifNoneMatch) {
    const candidates = normalizedEtags(ifNoneMatch);
    return candidates.includes('*')
      || candidates.includes(String(metadata.etag).replace(/^W\//u, ''));
  }
  const ifModifiedSince = readHeader('if-modified-since');
  if (!ifModifiedSince) return false;
  const requestTime = new Date(ifModifiedSince).getTime();
  const generatedTime = new Date(metadata.generatedAt).getTime();
  if (!Number.isFinite(requestTime) || !Number.isFinite(generatedTime)) return false;
  return Math.floor(generatedTime / 1000) <= Math.floor(requestTime / 1000);
}

export function createCalendarSingleFlight(options = {}) {
  const active = new Map();
  const run = (key, builder) => {
    const normalizedKey = clean(key, 500);
    if (active.has(normalizedKey)) {
      options.onCoalesced?.(normalizedKey);
      return active.get(normalizedKey);
    }
    const promise = Promise.resolve()
      .then(builder)
      .finally(() => {
        if (active.get(normalizedKey) === promise) active.delete(normalizedKey);
      });
    active.set(normalizedKey, promise);
    return promise;
  };
  run.activeCount = () => active.size;
  return run;
}

export { CALENDAR_RENDERER_SCHEMA_VERSION };
