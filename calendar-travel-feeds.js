const travelFeedDefinitions = Object.freeze({
  all: Object.freeze({
    key: 'all',
    cacheKind: 'travel',
    calendarName: 'Travel Calendar',
    calendarDescription: 'All travel events including flights, ground transportation, and hotels',
    calendarPath: '/calendar/travel',
    dataPath: '/travel/calendar',
    subscribePath: '/subscribe/travel',
    filename: 'travel-calendar.ics',
    preparationLabel: 'Travel calendar',
    subscribeDescription: 'View all flight, ground transportation, and hotel events in one calendar.',
  }),
  flights: Object.freeze({
    key: 'flights',
    cacheKind: 'travel-flights',
    calendarName: '✈️ Travel • Flights',
    calendarDescription: 'Downbeat flight departures, arrivals, and connections',
    calendarPath: '/calendar/travel-flights',
    dataPath: '/travel/flights/calendar',
    subscribePath: '/subscribe/travel-flights',
    filename: 'travel-flights-calendar.ics',
    preparationLabel: 'Travel Flights calendar',
    subscribeDescription: 'View flight departures, arrivals, and connections without hotel or ground transportation clutter.',
  }),
  ground: Object.freeze({
    key: 'ground',
    cacheKind: 'travel-ground',
    calendarName: '🚐 Travel • Ground',
    calendarDescription: 'Downbeat pickups, drop-offs, lobby calls, rentals, shuttles, and rideshares',
    calendarPath: '/calendar/travel-ground',
    dataPath: '/travel/ground/calendar',
    subscribePath: '/subscribe/travel-ground',
    filename: 'travel-ground-calendar.ics',
    preparationLabel: 'Travel Ground calendar',
    subscribeDescription: 'View pickups, drop-offs, lobby calls, rentals, shuttles, and rideshares in a focused calendar.',
  }),
  hotels: Object.freeze({
    key: 'hotels',
    cacheKind: 'travel-hotels',
    calendarName: '🏨 Travel • Hotels',
    calendarDescription: 'Downbeat hotel stays shown as compact all-day date bars',
    calendarPath: '/calendar/travel-hotels',
    dataPath: '/travel/hotels/calendar',
    subscribePath: '/subscribe/travel-hotels',
    filename: 'travel-hotels-calendar.ics',
    preparationLabel: 'Travel Hotels calendar',
    subscribeDescription: 'View each hotel stay as a compact all-day date bar with check-in and check-out times in the details.',
  }),
});

export const TRAVEL_FEED_KEYS = Object.freeze(['all', 'flights', 'ground', 'hotels']);

export function travelFeedDefinition(key = 'all') {
  return travelFeedDefinitions[key] || null;
}

export function travelFeedKeyFromSubscriptionPath(pathname) {
  for (const key of TRAVEL_FEED_KEYS) {
    if (travelFeedDefinitions[key].subscribePath === pathname) {
      return key;
    }
  }
  return null;
}

function normalizedEventType(event) {
  return String(event?.type || '').trim().toLowerCase();
}

function isFlightEvent(event) {
  return normalizedEventType(event).startsWith('flight_');
}

function isGroundEvent(event) {
  const type = normalizedEventType(event);
  return type.startsWith('transportation_') || type.startsWith('ground_transport');
}

function isHotelStayEvent(event) {
  const type = normalizedEventType(event);
  return type === 'hotel_checkin' || type === 'hotel_stay';
}

function feedScopedUid(uid, feed) {
  const text = String(uid || '').trim();
  if (!text) {
    return uid;
  }

  const suffix = `-travel-${feed}`;
  const atIndex = text.indexOf('@');
  if (atIndex === -1) {
    return `${text}${suffix}`;
  }

  return `${text.slice(0, atIndex)}${suffix}${text.slice(atIndex)}`;
}

function withFeedScopedUid(event, feed) {
  return {
    ...event,
    uid: feedScopedUid(event.uid, feed),
  };
}

function withEmojiPrefix(title, emoji, fallback) {
  const normalizedTitle = String(title || '').trim() || fallback;
  if (normalizedTitle.startsWith(`${emoji} `) || normalizedTitle === emoji) {
    return normalizedTitle;
  }
  return `${emoji} ${normalizedTitle}`;
}

function asDate(value) {
  const date = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function asUtcCalendarDate(value) {
  const date = asDate(value);
  if (!date) {
    return null;
  }
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function addUtcDays(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function formatFloatingDateTime(value) {
  const date = asDate(value);
  if (!date) {
    return 'Not provided';
  }

  const dateText = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  }).format(date);
  const timeText = new Intl.DateTimeFormat('en-US', {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZone: 'UTC',
  }).format(date);
  return `${dateText} at ${timeText}`;
}

function hotelStayTitle(title) {
  const normalizedTitle = String(title || '').trim().replace(/^hotel\s*:\s*/iu, '');
  return withEmojiPrefix(normalizedTitle, '🏨', 'Hotel Stay');
}

function hotelStayEvent(event) {
  const start = asUtcCalendarDate(event.start);
  if (!start) {
    return null;
  }

  let end = asUtcCalendarDate(event.end);
  if (!end || end.getTime() <= start.getTime()) {
    end = addUtcDays(start, 1);
  }

  const timing = [
    `Check-in: ${formatFloatingDateTime(event.start)}`,
    `Check-out: ${formatFloatingDateTime(event.end)}`,
  ].join('\n');

  return {
    ...event,
    type: 'hotel_stay',
    title: hotelStayTitle(event.title),
    start,
    end,
    allDay: true,
    description: [timing, String(event.description || '').trim()].filter(Boolean).join('\n\n'),
    uid: feedScopedUid(event.uid, 'hotels'),
  };
}

export function travelCalendarEventsForFeed(events, feed = 'all') {
  if (!travelFeedDefinition(feed)) {
    throw new Error(`Unknown Travel feed: ${feed}`);
  }

  const sourceEvents = Array.isArray(events) ? events : [];
  if (feed === 'all') {
    return [...sourceEvents];
  }
  if (feed === 'flights') {
    return sourceEvents.filter(isFlightEvent).map((event) => withFeedScopedUid(event, 'flights'));
  }
  if (feed === 'ground') {
    return sourceEvents.filter(isGroundEvent).map((event) => ({
      ...withFeedScopedUid(event, 'ground'),
      title: withEmojiPrefix(event.title, '🚐', 'Ground Transportation'),
    }));
  }

  return sourceEvents.filter(isHotelStayEvent).map(hotelStayEvent).filter(Boolean);
}
