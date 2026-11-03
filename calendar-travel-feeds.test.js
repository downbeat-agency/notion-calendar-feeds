import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ical from 'ical-generator';

import { serializeCalendarWithTimePolicy } from './calendar-ics-policy.js';

import {
  TRAVEL_FEED_KEYS,
  travelCalendarEventsForFeed,
  travelFeedDefinition,
  travelFeedKeyFromSubscriptionPath,
} from './calendar-travel-feeds.js';

const baseEvents = [
  {
    type: 'flight_departure',
    title: '✈️ Flight to SFO',
    start: '2026-10-10T09:00:00.000Z',
    end: '2026-10-10T10:30:00.000Z',
    uid: 'downbeat-flight@calendar.downbeat.agency',
  },
  {
    type: 'transportation_pickup',
    title: 'Lobby Call',
    start: '2026-10-10T08:00:00.000Z',
    end: '2026-10-10T08:30:00.000Z',
    uid: 'downbeat-ground@calendar.downbeat.agency',
  },
  {
    type: 'hotel_checkin',
    title: 'Hotel: Example Inn',
    start: '2026-10-10T15:00:00.000Z',
    end: '2026-10-12T11:00:00.000Z',
    description: 'Reservation: Test Guest',
    uid: 'downbeat-hotel@calendar.downbeat.agency',
  },
  {
    type: 'hotel_checkout',
    title: 'Example Inn - Check-out',
    start: '2026-10-12T11:00:00.000Z',
    end: '2026-10-12T12:00:00.000Z',
    uid: 'downbeat-checkout@calendar.downbeat.agency',
  },
];

const indexSource = readFileSync(new URL('./index.js', import.meta.url), 'utf8');

test('travel projector retains checkout time in the stay and does not emit a checkout event', () => {
  const travelSource = indexSource.split('function processTravelEvents(travelGroupsArray)')[1]
    .split('function serializeTravelFeedCalendar')[0];
  assert.match(travelSource, /Check-out: \$\{checkoutHasTime/u);
  assert.match(travelSource, /toLocaleString\('en-US', \{ timeZone: 'UTC'/u);
  assert.match(travelSource, /toLocaleDateString\('en-US', \{ timeZone: 'UTC'/u);
  assert.doesNotMatch(travelSource, /type: 'hotel_checkout'|checkout_occurrence_key/u);
});

test('Travel feed definitions retain Travel All and add three category feeds', () => {
  assert.deepEqual(TRAVEL_FEED_KEYS, ['all', 'flights', 'ground', 'hotels']);
  assert.deepEqual(
    TRAVEL_FEED_KEYS.map((key) => {
      const feed = travelFeedDefinition(key);
      return [key, feed.calendarName, feed.calendarPath, feed.dataPath, feed.subscribePath];
    }),
    [
      ['all', 'Travel Calendar', '/calendar/travel', '/travel/calendar', '/subscribe/travel'],
      ['flights', '✈️ Travel • Flights', '/calendar/travel-flights', '/travel/flights/calendar', '/subscribe/travel-flights'],
      ['ground', '🚐 Travel • Ground', '/calendar/travel-ground', '/travel/ground/calendar', '/subscribe/travel-ground'],
      ['hotels', '🏨 Travel • Hotels', '/calendar/travel-hotels', '/travel/hotels/calendar', '/subscribe/travel-hotels'],
    ]
  );
  assert.equal(travelFeedKeyFromSubscriptionPath('/subscribe/travel'), 'all');
  assert.equal(travelFeedKeyFromSubscriptionPath('/subscribe/travel-ground'), 'ground');
  assert.equal(travelFeedKeyFromSubscriptionPath('/subscribe/unknown'), null);
});

test('Travel All omits retired checkout occurrences while preserving stay and other travel UIDs', () => {
  const events = travelCalendarEventsForFeed(baseEvents, 'all');
  assert.deepEqual(events, baseEvents.slice(0, 3));
  const calendar = ical({ name: 'Travel Calendar' });
  for (const event of events) {
    calendar.createEvent({
      id: event.uid,
      start: new Date(event.start),
      end: new Date(event.end),
      summary: event.title,
      description: event.description,
      floating: true,
    });
  }
  const ics = serializeCalendarWithTimePolicy(calendar, { mode: 'floating' });
  assert.match(ics, /UID:downbeat-hotel@calendar\.downbeat\.agency/u);
  assert.match(ics, /UID:downbeat-flight@calendar\.downbeat\.agency/u);
  assert.match(ics, /UID:downbeat-ground@calendar\.downbeat\.agency/u);
  assert.doesNotMatch(ics, /downbeat-checkout|Check-out/u);
});

test('hotel feed keeps multi-night, same-day, missing checkout and DST boundary stays as one occurrence', () => {
  const cases = [
    ['multi-night', '2026-10-10T15:00:00.000Z', '2026-10-12T11:00:00.000Z', '2026-10-12T00:00:00.000Z'],
    ['same-day', '2026-10-10T08:00:00.000Z', '2026-10-10T20:00:00.000Z', '2026-10-11T00:00:00.000Z'],
    ['missing checkout', '2026-10-10T15:00:00.000Z', null, '2026-10-11T00:00:00.000Z'],
    ['spring DST', '2026-03-07T15:00:00.000Z', '2026-03-09T11:00:00.000Z', '2026-03-09T00:00:00.000Z'],
    ['fall DST', '2026-10-31T15:00:00.000Z', '2026-11-02T11:00:00.000Z', '2026-11-02T00:00:00.000Z'],
  ];
  for (const [label, start, end, expectedEnd] of cases) {
    const source = { ...baseEvents[2], start, end };
    const all = travelCalendarEventsForFeed([source, baseEvents[3]], 'all');
    assert.equal(all.length, 1, label);
    assert.equal(all[0].uid, source.uid, label);
    const hotels = travelCalendarEventsForFeed([source, baseEvents[3]], 'hotels');
    assert.equal(hotels.length, 1, label);
    assert.equal(hotels[0].end.toISOString(), expectedEnd, label);
    assert.equal(hotels[0].uid, 'downbeat-hotel-travel-hotels@calendar.downbeat.agency', label);
    assert.match(hotels[0].description, /Check-out: /u, label);
  }
});

test('Flights feed includes only flight occurrences with feed-scoped UIDs', () => {
  const events = travelCalendarEventsForFeed(baseEvents, 'flights');
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'flight_departure');
  assert.equal(events[0].title, '✈️ Flight to SFO');
  assert.equal(events[0].uid, 'downbeat-flight-travel-flights@calendar.downbeat.agency');
});

test('Ground feed includes ground occurrences and gives them a visible emoji', () => {
  const events = travelCalendarEventsForFeed([
    ...baseEvents,
    { type: 'ground_transport_dropoff', title: '🚐 Drop Off', uid: 'ground-two' },
  ], 'ground');
  assert.deepEqual(events.map((event) => event.title), ['🚐 Lobby Call', '🚐 Drop Off']);
  assert.deepEqual(events.map((event) => event.uid), [
    'downbeat-ground-travel-ground@calendar.downbeat.agency',
    'ground-two-travel-ground',
  ]);
});

test('Hotels feed turns each stay into one all-day bar with timing details', () => {
  const events = travelCalendarEventsForFeed(baseEvents, 'hotels');
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'hotel_stay');
  assert.equal(events[0].title, '🏨 Example Inn');
  assert.equal(events[0].allDay, true);
  assert.equal(events[0].start.toISOString(), '2026-10-10T00:00:00.000Z');
  assert.equal(events[0].end.toISOString(), '2026-10-12T00:00:00.000Z');
  assert.match(events[0].description, /Check-in: Oct 10, 2026 at 3:00 PM/u);
  assert.match(events[0].description, /Check-out: Oct 12, 2026 at 11:00 AM/u);
  assert.match(events[0].description, /Reservation: Test Guest/u);
  assert.equal(events[0].uid, 'downbeat-hotel-travel-hotels@calendar.downbeat.agency');
});

test('hotel stay dates reach ICS as compact all-day values', () => {
  const [hotel] = travelCalendarEventsForFeed(baseEvents, 'hotels');
  const calendar = ical({ name: 'Hotels' });
  calendar.createEvent({
    id: hotel.uid,
    start: hotel.start,
    end: hotel.end,
    summary: hotel.title,
    description: hotel.description,
    floating: true,
    allDay: hotel.allDay,
  });
  const ics = serializeCalendarWithTimePolicy(calendar, { mode: 'floating' });
  assert.match(ics, /DTSTART;VALUE=DATE:20261010/u);
  assert.match(ics, /DTEND;VALUE=DATE:20261012/u);
  assert.match(ics, /SUMMARY:🏨 Example Inn/u);
  assert.doesNotMatch(ics, /DTSTART:20261010T150000/u);
});

test('all category routes and authenticated regeneration routes are wired', () => {
  for (const route of [
    '/travel/flights/calendar',
    '/travel/ground/calendar',
    '/travel/hotels/calendar',
    '/calendar/travel-flights.ics',
    '/calendar/travel-ground.ics',
    '/calendar/travel-hotels.ics',
    '/subscribe/travel-flights',
    '/subscribe/travel-ground',
    '/subscribe/travel-hotels',
  ]) {
    assert.ok(indexSource.includes(route), route);
  }

  for (const route of [
    '/travel/calendar/regen',
    '/travel/flights/calendar/regen',
    '/travel/ground/calendar/regen',
    '/travel/hotels/calendar/regen',
  ]) {
    assert.ok(
      indexSource.includes(`app.post('${route}', requireCalendarFeedServiceKey`),
      `${route} must require service authentication`
    );
  }
  assert.match(indexSource, /feedKey === 'all' \? TRAVEL_FEED_KEYS : \[feedKey\]/u);
});

test('unknown Travel feed keys fail closed', () => {
  assert.equal(travelFeedDefinition('unknown'), null);
  assert.throws(
    () => travelCalendarEventsForFeed(baseEvents, 'unknown'),
    /Unknown Travel feed/u
  );
});
