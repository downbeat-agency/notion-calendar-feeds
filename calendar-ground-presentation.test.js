import test from 'node:test';
import assert from 'node:assert/strict';
import ical from 'ical-generator';
import { groundStopCalendarTitle, groundStopCalendarDescription } from './calendar-ground-presentation.js';

test('stop title is only its saved name, with a safe blank fallback', () => {
  assert.equal(groundStopCalendarTitle('  Airport shuttle to hotel  '), 'Airport shuttle to hotel');
  assert.equal(groundStopCalendarTitle(' \n '), 'Ground transportation');
  assert.equal(groundStopCalendarTitle(null), 'Ground transportation');
});

test('stop notes stay multiline and precede useful existing details', () => {
  const notes = 'Wait for the last passenger.\nCollect luggage, then call dispatch.';
  assert.equal(
    groundStopCalendarDescription(notes, 'Drivers:\n- Diego\n\nGround Details: https://example.test/ground'),
    `${notes}\n\nDrivers:\n- Diego\n\nGround Details: https://example.test/ground`
  );
  assert.equal(groundStopCalendarDescription(notes, 'Ground transportation details'), notes);
  assert.equal(groundStopCalendarDescription('  ', 'Ground transportation details'), 'Ground transportation details');
});

test('stop presentation serializes to ICS summary and notes without changing identity or location', () => {
  const calendar = ical({ name: 'Downbeat' });
  calendar.createEvent({
    id: 'stable-stop@calendar.downbeat.agency',
    start: new Date('2026-10-03T16:00:00Z'),
    end: new Date('2026-10-03T17:00:00Z'),
    summary: groundStopCalendarTitle('Airport shuttle to hotel'),
    description: groundStopCalendarDescription('Wait for final passenger.\nCall dispatch.', 'Ground transportation details'),
    location: '9800 Airport Blvd',
  });
  const output = calendar.toString();
  assert.match(output, /SUMMARY:Airport shuttle to hotel/u);
  assert.match(output, /DESCRIPTION:Wait for final passenger\.\\nCall dispatch\./u);
  assert.match(output, /LOCATION:9800 Airport Blvd/u);
  assert.match(output, /UID:stable-stop@calendar\.downbeat\.agency/u);
});
