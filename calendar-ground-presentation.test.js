import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ical from 'ical-generator';
import {
  groundStopCalendarTitle,
  groundStopCalendarDescription,
  groundTravelDropOffPresentation,
  groundTravelOccurrencePeople,
  groundTravelRoleDescription,
} from './calendar-ground-presentation.js';

test('stop title is only its saved name, with a safe blank fallback', () => {
  assert.equal(groundStopCalendarTitle('  Airport shuttle to hotel  '), 'Airport shuttle to hotel');
  assert.equal(groundStopCalendarTitle(' \n '), 'Ground transportation');
  assert.equal(groundStopCalendarTitle(null), 'Ground transportation');
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

test('Travel ground events prefer their own stop participants over journey-wide fallbacks', () => {
  const transport = {
    personnel: { personnel_name: ['Legacy Driver'] },
    drivers: ['Legacy Driver'],
    passengers: [],
    journey_drivers: ['Pickup Driver', 'Drop-off Driver'],
    journey_passengers: ['Trip Passenger'],
    pickup_personnel: { personnel_name: ['Pickup Driver'] },
    pickup_drivers: ['Pickup Driver'],
    pickup_passengers: [],
    drop_off_personnel: { personnel_name: ['Drop-off Driver'] },
    drop_off_drivers: ['Drop-off Driver'],
    drop_off_passengers: [],
  };
  assert.deepEqual(groundTravelOccurrencePeople(transport, 'pickup'), {
    personnel: ['Pickup Driver'],
    drivers: ['Pickup Driver'],
    passengers: [],
    journeyDrivers: ['Pickup Driver', 'Drop-off Driver'],
    journeyPassengers: ['Trip Passenger'],
  });
  assert.deepEqual(groundTravelOccurrencePeople(transport, 'dropoff'), {
    personnel: ['Drop-off Driver'],
    drivers: ['Drop-off Driver'],
    passengers: [],
    journeyDrivers: ['Pickup Driver', 'Drop-off Driver'],
    journeyPassengers: ['Trip Passenger'],
  });
});

test('Travel ground stop participant fields are backward compatible and honor explicit empties', () => {
  assert.deepEqual(groundTravelOccurrencePeople({
    personnel: { personnel_name: ['Legacy Passenger'] },
    drivers: [],
    passengers: ['Legacy Passenger'],
  }, 'pickup'), {
    personnel: ['Legacy Passenger'],
    drivers: [],
    passengers: ['Legacy Passenger'],
    journeyDrivers: [],
    journeyPassengers: ['Legacy Passenger'],
  });
  assert.deepEqual(groundTravelOccurrencePeople({
    personnel: { personnel_name: ['Stale Driver'] },
    drivers: ['Stale Driver'],
    passengers: ['Stale Passenger'],
    drop_off_personnel: { personnel_name: [] },
    drop_off_drivers: [],
    drop_off_passengers: [],
  }, 'dropoff'), {
    personnel: [],
    drivers: [],
    passengers: [],
    journeyDrivers: [],
    journeyPassengers: [],
  });
});

test('Travel ground roles distinguish this stop from later trip passengers', () => {
  assert.equal(
    groundTravelRoleDescription({
      personnel: ['Pickup Driver'],
      drivers: ['Pickup Driver'],
      passengers: [],
      journeyDrivers: ['Pickup Driver'],
      journeyPassengers: ['Trip Passenger One', 'Trip Passenger Two'],
    }),
    '🚘 Driver:\nPickup Driver\n\n🧳 Trip Passengers:\nTrip Passenger One\nTrip Passenger Two'
  );
  assert.equal(
    groundTravelRoleDescription({
      personnel: ['Pickup Driver'],
      drivers: ['Pickup Driver'],
      passengers: [],
      journeyDrivers: ['Pickup Driver'],
      journeyPassengers: [],
    }),
    '🚘 Driver:\nPickup Driver\n\n🧳 Passengers:\nNone assigned at this stop'
  );
});

test('global Travel ground renderer resolves pickup and drop-off people independently', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  const travelRenderer = source.slice(source.indexOf('function processTravelEvents('));
  assert.match(travelRenderer, /groundTravelOccurrencePeople\(transport, 'pickup'\)/u);
  assert.match(travelRenderer, /groundTravelOccurrencePeople\(transport, 'dropoff'\)/u);
  assert.match(travelRenderer, /groundTravelRoleDescription\(pickupPeople\)/u);
  assert.match(travelRenderer, /groundTravelRoleDescription\(dropOffPeople\)/u);
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
