import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ical from 'ical-generator';
import { calendarTravelDescription } from './calendar-travel-notes.js';

test('flight and hotel notes retain multiline text and generated booking details', () => {
  assert.equal(
    calendarTravelDescription('Confirmation: ABC123', 'Keep boarding pass.\nCheck bags early.', 'Meet at gate 4.', 'Flight Notes'),
    'Booking Notes:\nKeep boarding pass.\nCheck bags early.\n\nFlight Notes:\nMeet at gate 4.\n\nConfirmation: ABC123'
  );
  assert.equal(
    calendarTravelDescription('Hotel Stay\nConfirmation: H123', 'Late arrival', 'Use side entrance', 'Property Notes'),
    'Booking Notes:\nLate arrival\n\nProperty Notes:\nUse side entrance\n\nHotel Stay\nConfirmation: H123'
  );
  assert.equal(calendarTravelDescription('Confirmation: ABC123', ' \n ', null, 'Flight Notes'), 'Confirmation: ABC123');
});

test('new Notes content serializes in ICS without changing event identity or location', () => {
  const calendar = ical({ name: 'Downbeat' });
  calendar.createEvent({
    id: 'stable-leg@calendar.downbeat.agency',
    start: new Date('2026-10-03T16:00:00Z'),
    end: new Date('2026-10-03T17:00:00Z'),
    summary: 'Flight to BOS',
    description: calendarTravelDescription('Confirmation: ABC123', 'Keep boarding pass.\nCheck bags early.', 'Meet at gate 4.', 'Flight Notes'),
    location: 'LAX',
  });
  const output = calendar.toString().replace(/\r\n[ \t]/gu, '');
  assert.match(output, /UID:stable-leg@calendar\.downbeat\.agency/u);
  assert.match(output, /LOCATION:LAX/u);
  assert.match(output, /DESCRIPTION:Booking Notes:\\nKeep boarding pass\.\\nCheck bags early\./u);
  assert.match(output, /Flight Notes:\\nMeet at gate 4\./u);
  assert.match(output, /Confirmation: ABC123/u);
});

test('personnel and Travel renderers use notes while Admin remains on its existing paths', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  const personal = source.slice(source.indexOf('function buildCalendarEventsFromCalendarData('), source.indexOf('function processAdminEvents('));
  const admin = source.slice(source.indexOf('function processAdminEvents('), source.indexOf('function processTravelEvents('));
  const travel = source.slice(source.indexOf('function processTravelEvents('));
  assert.match(personal, /calendarTravelDescription\([^\n]+hotel\.booking_notes, hotel\.property_notes, 'Property Notes'\)/u);
  assert.match(source, /appendFlightAwareDetails\(desc\.trim\(\), flight, legType\)[\s\S]+flight\.booking_notes, flight\.leg_notes, 'Flight Notes'/u);
  assert.match(travel, /calendarTravelDescription\(description\.trim\(\), flight\.booking_notes, flight\.leg_notes, 'Flight Notes'\)/u);
  assert.match(travel, /calendarTravelDescription\(description\.trim\(\), hotel\.booking_notes, hotel\.property_notes, 'Property Notes'\)/u);
  assert.doesNotMatch(admin, /calendarTravelDescription/u);
});
