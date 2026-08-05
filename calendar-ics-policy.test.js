import assert from 'node:assert/strict';
import test from 'node:test';
import ical from 'ical-generator';

import {
  configuredCalendarTimeMode,
  serializeCalendarWithTimePolicy,
  serializeGoogleCalendarWithTimePolicy,
} from './calendar-ics-policy.js';

function floatingCalendar() {
  const calendar = ical({ name: 'Floating test' });
  calendar.createEvent({
    id: 'italy-test',
    start: new Date(Date.UTC(2026, 7, 5, 15, 0, 0)),
    end: new Date(Date.UTC(2026, 7, 5, 16, 0, 0)),
    summary: 'Italy arrival',
    floating: true,
  });
  return calendar;
}

test('floating is the default time policy with a reversible legacy option', () => {
  assert.equal(configuredCalendarTimeMode({}), 'floating');
  assert.equal(configuredCalendarTimeMode({ CALENDAR_TIME_MODE: 'legacy-la' }), 'legacy-la');
  assert.throws(
    () => configuredCalendarTimeMode({ CALENDAR_TIME_MODE: 'utc' }),
    /floating or legacy-la/u
  );
});

test('Apple and Google artifacts preserve the same floating clock time', () => {
  const apple = serializeCalendarWithTimePolicy(floatingCalendar(), { mode: 'floating' });
  const google = serializeGoogleCalendarWithTimePolicy(floatingCalendar(), { mode: 'floating' });
  for (const icsData of [apple, google]) {
    assert.match(icsData, /DTSTART:20260805T150000/u);
    assert.match(icsData, /DTEND:20260805T160000/u);
    assert.doesNotMatch(icsData, /TZID=|X-WR-TIMEZONE|BEGIN:VTIMEZONE/u);
  }
});

test('legacy Los Angeles mode remains available for instant rollback', () => {
  const google = serializeGoogleCalendarWithTimePolicy(floatingCalendar(), {
    mode: 'legacy-la',
  });
  assert.match(google, /DTSTART;TZID=America\/Los_Angeles:20260805T150000/u);
  assert.match(google, /BEGIN:VTIMEZONE/u);
});

test('true all-day blockouts serialize as date values with an exclusive end', () => {
  const calendar = ical({ name: 'Blockouts' });
  calendar.createEvent({
    id: 'blockout-test',
    start: new Date(Date.UTC(2026, 7, 5)),
    end: new Date(Date.UTC(2026, 7, 7)),
    summary: 'Blockout',
    floating: true,
    allDay: true,
  });
  const result = serializeCalendarWithTimePolicy(calendar, { mode: 'floating' });
  assert.match(result, /DTSTART;VALUE=DATE:20260805/u);
  assert.match(result, /DTEND;VALUE=DATE:20260807/u);
  assert.doesNotMatch(result, /DTSTART:20260805T000000/u);
});
