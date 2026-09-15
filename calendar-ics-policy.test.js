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

test('standard iCal remains floating while Google specifies Pacific time', () => {
  const apple = serializeCalendarWithTimePolicy(floatingCalendar(), { mode: 'floating' });
  const google = serializeGoogleCalendarWithTimePolicy(floatingCalendar(), { mode: 'floating' });
  assert.match(apple, /DTSTART:20260805T150000/u);
  assert.doesNotMatch(apple, /TZID=|X-WR-TIMEZONE|BEGIN:VTIMEZONE/u);
  assert.match(google, /DTSTART;TZID=America\/Los_Angeles:20260805T150000/u);
  assert.match(google, /X-WR-TIMEZONE:America\/Los_Angeles/u);
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


test('Kevin departure keeps Saturday 05:30 and preserves event identity', () => {
  const calendar = ical({ name: 'Departure regression' });
  calendar.createEvent({ id: 'stable-departure', start: new Date(Date.UTC(2026, 8, 19, 5, 30)),
    end: new Date(Date.UTC(2026, 8, 19, 6)), summary: 'Departure', floating: true });
  const result = serializeGoogleCalendarWithTimePolicy(calendar);
  assert.match(result, /DTSTART;TZID=America\/Los_Angeles:20260919T053000/u);
  assert.match(result, /DTEND;TZID=America\/Los_Angeles:20260919T060000/u);
  assert.match(result, /UID:stable-departure/u);
  assert.match(result, /TZOFFSETTO:-0700/u);
  assert.match(result, /TZOFFSETTO:-0800/u);
  assert.match(result, /RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU/u);
  assert.match(result, /RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU/u);
});

test('Google preserves UTC instants, explicit travel zones and all-day ranges', () => {
  const source = 'BEGIN:VCALENDAR\r\nVERSION:2.0\r\n'
    + 'BEGIN:VEVENT\r\nDTSTART:20260919T123000Z\r\nDTEND:20260919T133000Z\r\nEND:VEVENT\r\n'
    + 'BEGIN:VEVENT\r\nDTSTART;TZID=America/New_York:20260919T160000\r\nEND:VEVENT\r\n'
    + 'BEGIN:VEVENT\r\nDTSTART;VALUE=DATE:20260919\r\nDTEND;VALUE=DATE:20260921\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n';
  const result = serializeGoogleCalendarWithTimePolicy({ toString: () => source });
  assert.match(result, /DTSTART:20260919T123000Z/u);
  assert.match(result, /DTEND:20260919T133000Z/u);
  assert.match(result, /DTSTART;TZID=America\/New_York:20260919T160000/u);
  assert.match(result, /DTSTART;VALUE=DATE:20260919/u);
  assert.match(result, /DTEND;VALUE=DATE:20260921/u);
  assert.doesNotMatch(result, /TZID=[^\r\n]+Z\r/u);
});
