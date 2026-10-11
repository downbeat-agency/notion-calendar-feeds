import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ical from 'ical-generator';
import {
  groundStopCalendarTitle,
  groundStopCalendarDescription,
  groundStopPersonnelDescription,
  groundTravelDropOffPresentation,
} from './calendar-ground-presentation.js';

test('ground title has exactly one van across updates and legacy vehicle prefixes', () => {
  assert.equal(groundStopCalendarTitle('  Airport shuttle to hotel  '), '🚐 Airport shuttle to hotel');
  assert.equal(groundStopCalendarTitle('🚐 🚐 Airport shuttle to hotel'), '🚐 Airport shuttle to hotel');
  assert.equal(groundStopCalendarTitle('🚐️ 🚐 Pickup'), '🚐 Pickup');
  assert.equal(groundStopCalendarTitle('🚙 Passenger drop-off'), '🚐 Passenger drop-off');
  assert.equal(groundStopCalendarTitle(' \n '), '🚐 Ground transportation');
  assert.equal(groundStopCalendarTitle(null), '🚐 Ground transportation');
});

test('Travel drop-off uses the action as its title and the street address as its location', () => {
  assert.deepEqual(
    groundTravelDropOffPresentation({
      transportation_name: 'Audio Drive',
      drop_off_name: 'DBLA',
      drop_off_address: '123 W Bellevue Dr Ste 4 Pasadena CA 91105',
    }),
    {
      title: 'Drop Off',
      location: '123 W Bellevue Dr Ste 4 Pasadena CA 91105',
    }
  );
  assert.deepEqual(
    groundTravelDropOffPresentation({ drop_off_name: 'DBLA' }),
    { title: 'Drop Off', location: 'DBLA' }
  );
});

test('global Travel drop-off renderer uses the action and address presentation', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  const travelRenderer = source.slice(source.indexOf('function processTravelEvents('));
  assert.match(travelRenderer, /const dropOffPresentation = groundTravelDropOffPresentation\(transport\)/u);
  assert.match(travelRenderer, /title: dropOffPresentation\.title/u);
  assert.match(travelRenderer, /location: dropOffPresentation\.location/u);
  assert.doesNotMatch(travelRenderer, /`\$\{transport\.transportation_name\} - Drop-off`/u);
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
    description: groundStopPersonnelDescription({
      stop_participants: [{ role: 'driver', name: 'Diego' }, { role: 'passenger', name: 'Alex' }],
      notes: 'Wait for final passenger.\nCall dispatch.',
    }),
    location: '9800 Airport Blvd',
  });
  const output = calendar.toString();
  const unfolded = output.replace(/\r\n /gu, '');
  assert.match(output, /SUMMARY:🚐 Airport shuttle to hotel/u);
  assert.match(unfolded, /DESCRIPTION:Drivers:\\n- Diego\\n\\nPassengers:\\n- Alex\\n\\nStop Notes:\\nWait for final passenger\.\\nCall dispatch\./u);
  assert.match(output, /LOCATION:9800 Airport Blvd/u);
  assert.match(output, /UID:stable-stop@calendar\.downbeat\.agency/u);
});

test('stop descriptions use only that stop, omit missing fields and private phones', () => {
  const first = groundStopPersonnelDescription({
    stop_participants: [{ role: 'driver', name: 'Diego', phone: 'private' },
      { role: 'passenger', name: 'Alex' }], notes: 'Load cases',
  });
  const second = groundStopPersonnelDescription({
    stop_participants: [{ role: 'driver', name: 'Sam' }, { role: 'passenger', name: '' }],
    notes: 'Use rear entrance',
  });
  assert.match(first, /Drivers:\n- Diego\n\nPassengers:\n- Alex\n\nStop Notes:\nLoad cases/u);
  assert.doesNotMatch(first, /private|Sam|rear entrance/u);
  assert.equal(second, 'Drivers:\n- Sam\n\nStop Notes:\nUse rear entrance');
  assert.equal(groundStopPersonnelDescription({ stop_participants: [] }), 'Ground transportation details');
  assert.equal(groundStopPersonnelDescription({ stop_participants: [],
    description: { Drivers: [{ driver_name: 'Unrelated trip driver' }] },
  }), 'Ground transportation details');
});

test('personnel renderer uses the stop-specific description for both event and top-level stops', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  const renderer = source.slice(source.indexOf('function buildCalendarEventsFromCalendarData('),
    source.indexOf('async function regenerateCalendarForPersonFromNotion('));
  assert.match(source, /if \(Array\.isArray\(transport\?\.stop_participants\)\) \{\s*return groundStopPersonnelDescription\(transport\)/u);
  assert.equal((renderer.match(/description: buildTransportDescription\(transport\)/gu) || []).length, 2);
  assert.equal((renderer.match(/const title = normalizeTransportTitle\(transport\.title\)/gu) || []).length, 2);
});

test('ICS escapes stop punctuation and folds long UTF-8 description lines', () => {
  const description = groundStopPersonnelDescription({
    stop_participants: [{ role: 'passenger', name: 'José, Jr.; ' + 'É'.repeat(55) }],
    notes: 'Gate A, then B; call dispatch\\now',
  });
  const calendar = ical({ name: 'Downbeat' });
  calendar.createEvent({ id: 'stable-stop@calendar.downbeat.agency',
    start: new Date('2026-10-03T16:00:00Z'), end: new Date('2026-10-03T17:00:00Z'),
    summary: groundStopCalendarTitle('Pickup'), description });
  const output = calendar.toString();
  const unfolded = output.replace(/\r\n /gu, '');
  assert.match(unfolded, /José\\, Jr\.\\;/u);
  assert.match(unfolded, /Gate A\\, then B\\; call dispatch/u);
  assert.match(output, /\r\n /u);
  assert.ok(output.split('\r\n').every((line) => Buffer.byteLength(line, 'utf8') <= 75));
});
