import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assertCalendarEventSnapshotCoverage,
  assertCalendarEventSnapshotExpectedIds,
  calendarEventOccurrenceSetSignature,
} from './calendar-event-snapshot.js';

test('calendar event coverage ignores order and volatile rendered fields', () => {
  const primary = [
    { event_name: 'Second', event_date_helper: '2026-08-03', calltime: '09:00' },
    { event_name: 'First', event_date_helper: '2026-08-02', calltime: '08:00' },
  ];
  const reference = [
    { event_name: 'First', event_date_helper: '2026-08-02', calltime: '' },
    { event_name: 'Second', event_date_helper: '2026-08-03', calltime: '' },
  ];
  assert.deepEqual(
    assertCalendarEventSnapshotCoverage(primary, reference),
    calendarEventOccurrenceSetSignature(primary)
  );
});

test('calendar event coverage rejects a repeated false-empty primary snapshot', () => {
  assert.throws(
    () => assertCalendarEventSnapshotCoverage([], [
      { event_name: 'Visible in Calendar Data', event_date_helper: '2026-08-03' },
    ]),
    (error) => error.code === 'NOTION_CALENDAR_EVENT_REFERENCE_MISMATCH'
  );
});

test('calendar event coverage accepts a genuinely empty calendar', () => {
  assert.deepEqual(assertCalendarEventSnapshotCoverage([], []), {
    count: 0,
    hash: '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',
  });
});

test('calendar event membership witness accepts the exact Notion event IDs', () => {
  const first = '30b39e4a-65a9-8021-8287-fd5a5f0d206f';
  const second = '39f39e4a-65a9-807b-8cd6-f86143aea5ba';
  assert.deepEqual(
    assertCalendarEventSnapshotExpectedIds([
      { notion_url: `https://www.notion.so/${second.replaceAll('-', '')}` },
      { notion_url: `https://www.notion.so/downbeat/${first.replaceAll('-', '')}` },
    ], [first, second]),
    { count: 2, expectedIds: [first, second] }
  );
});

test('calendar event membership witness rejects a stable partial formula', () => {
  assert.throws(
    () => assertCalendarEventSnapshotExpectedIds([], [
      '30b39e4a-65a9-8021-8287-fd5a5f0d206f',
    ]),
    (error) => error.code === 'NOTION_CALENDAR_EVENT_MEMBERSHIP_MISMATCH'
  );
});

test('calendar event membership witness uses exact count when legacy URLs are omitted', () => {
  assert.equal(
    assertCalendarEventSnapshotExpectedIds([
      { event_name: 'Legacy event without URL' },
    ], ['30b39e4a-65a9-8021-8287-fd5a5f0d206f']).count,
    1
  );
});
