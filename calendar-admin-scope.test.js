import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { adminCalendarEventsOnly } from './calendar-admin-scope.js';

test('Admin calendar keeps only main events and rehearsals', () => {
  const events = [
    { type: 'main_event', title: 'Event' },
    { type: 'rehearsal', title: 'Rehearsal' },
    { type: 'ground_transport_pickup', title: 'Pickup' },
    { type: 'ground_transport_dropoff', title: 'Drop-off' },
    { type: 'ground_transport_meeting', title: 'Lobby Call' },
    { type: 'ground_transport_passenger_dropoff', title: 'Passenger Drop Off' },
    { type: 'flight_departure', title: 'Flight' },
    { type: 'hotel', title: 'Hotel' },
  ];

  assert.deepEqual(
    adminCalendarEventsOnly(events).map(({ type, title }) => ({ type, title })),
    [
      { type: 'main_event', title: 'Event' },
      { type: 'rehearsal', title: 'Rehearsal' },
    ]
  );
});

test('Admin renderer applies the scope policy and does not create ground travel rows', async () => {
  const source = await readFile(new URL('./index.js', import.meta.url), 'utf8');
  const start = source.indexOf('function processAdminEvents(eventsArray)');
  const end = source.indexOf('// TRAVEL CALENDAR FUNCTIONS', start);
  const renderer = source.slice(start, end);

  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  assert.match(
    renderer,
    /return adminCalendarEventsOnly\(allCalendarEvents\)\.map\(calendarEventWithEventHubLink\);/u
  );
  assert.doesNotMatch(renderer, /event\.ground_transport/u);
});
