import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assertCalendarEventSnapshotCoverage,
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
