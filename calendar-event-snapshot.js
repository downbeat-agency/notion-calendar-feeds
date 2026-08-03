import { createHash } from 'node:crypto';

function clean(value) {
  return String(value ?? '').trim();
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
