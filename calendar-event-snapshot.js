import { createHash } from 'node:crypto';

function clean(value) {
  return String(value ?? '').trim();
}

function normalizeNotionPageId(value) {
  const compact = clean(value).toLowerCase().replaceAll('-', '');
  if (!/^[0-9a-f]{32}$/u.test(compact)) return null;
  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20),
  ].join('-');
}

function notionPageIdFromUrl(value) {
  const matches = clean(value).match(/[0-9a-f]{32}/giu) || [];
  return normalizeNotionPageId(matches.at(-1));
}

function expectedIdsError(message) {
  const error = new Error(message);
  error.code = 'NOTION_CALENDAR_EVENT_MEMBERSHIP_MISMATCH';
  return error;
}

function occurrenceKey(row = {}) {
  const key = [
    clean(row.event_name),
    clean(row.event_date_helper),
    clean(row.notion_url),
  ];
  if (key.every((value) => !value)) {
    const error = new Error('Calendar event snapshot row has no stable occurrence identity.');
    error.code = 'NOTION_CALENDAR_EVENT_REFERENCE_UNVERIFIABLE';
    throw error;
  }
  return JSON.stringify(key);
}

export function calendarEventOccurrenceSetSignature(rows = []) {
  if (!Array.isArray(rows)) {
    const error = new Error('Calendar event snapshot must be an array.');
    error.code = 'NOTION_CALENDAR_EVENT_REFERENCE_INVALID';
    throw error;
  }
  const keys = rows.map(occurrenceKey).sort();
  return {
    count: keys.length,
    hash: createHash('sha256').update(JSON.stringify(keys)).digest('hex'),
  };
}

export function assertCalendarEventSnapshotCoverage(primaryRows, referenceRows) {
  const primary = calendarEventOccurrenceSetSignature(primaryRows);
  const reference = calendarEventOccurrenceSetSignature(referenceRows);
  if (primary.count !== reference.count || primary.hash !== reference.hash) {
    const error = new Error('Notion calendar event snapshot is incomplete.');
    error.code = 'NOTION_CALENDAR_EVENT_REFERENCE_MISMATCH';
    throw error;
  }
  return primary;
}

export function assertCalendarEventSnapshotExpectedIds(rows, expectedEventIds) {
  if (!Array.isArray(rows) || !Array.isArray(expectedEventIds)) {
    throw expectedIdsError('Notion calendar event membership snapshot is invalid.');
  }

  const expected = [...new Set(expectedEventIds.map(normalizeNotionPageId))]
    .filter(Boolean)
    .sort();
  if (expected.length !== expectedEventIds.length) {
    throw expectedIdsError('Expected Notion calendar event membership contains invalid or duplicate IDs.');
  }
  if (rows.length !== expected.length) {
    throw expectedIdsError(
      `Notion Calendar Data rendered ${rows.length} events; membership witness requires ${expected.length}.`
    );
  }

  const renderedIds = rows.map((row) => notionPageIdFromUrl(row?.notion_url));
  // Some legacy Calendar Data rows intentionally omit notion_url. The exact
  // independently-derived count still proves completeness in that case.
  if (renderedIds.every(Boolean)) {
    const actual = [...new Set(renderedIds)].sort();
    if (actual.length !== rows.length || JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw expectedIdsError('Notion Calendar Data rendered the wrong event membership.');
    }
  }

  return { count: rows.length, expectedIds: expected };
}
