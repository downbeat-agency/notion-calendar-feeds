import { calendarPersonnelTravelDescriptionUrl } from './calendar-event-links.js';

export function groundStopCalendarTitle(value) {
  const title = typeof value === 'string' ? value.trim() : '';
  return `🚐 ${title.replace(/^(?:(?:🚐|🚙|🚗|🚕)\uFE0F?\s*)+/gu, '').trim() || 'Ground transportation'}`;
}

export function groundTravelDropOffPresentation(transport = {}) {
  const address = typeof transport.drop_off_address === 'string'
    ? transport.drop_off_address.trim()
    : '';
  const name = typeof transport.drop_off_name === 'string'
    ? transport.drop_off_name.trim()
    : '';
  return {
    title: 'Drop Off',
    location: address || name,
  };
}

function stringList(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value
    .map((entry) => (typeof entry === 'string' ? entry.trim() : ''))
    .filter(Boolean))];
}

export function groundTravelOccurrencePeople(transport = {}, occurrence = 'pickup') {
  const prefix = occurrence === 'dropoff' ? 'drop_off' : 'pickup';
  const scopedPersonnel = transport[`${prefix}_personnel`]?.personnel_name;
  const scopedDrivers = transport[`${prefix}_drivers`];
  const scopedPassengers = transport[`${prefix}_passengers`];
  const hasScopedDrivers = Array.isArray(scopedDrivers);
  const hasScopedPassengers = Array.isArray(scopedPassengers);
  return {
    personnel: stringList(Array.isArray(scopedPersonnel)
      ? scopedPersonnel
      : transport.personnel?.personnel_name),
    drivers: stringList(hasScopedDrivers ? scopedDrivers : transport.drivers),
    passengers: stringList(hasScopedPassengers ? scopedPassengers : transport.passengers),
    journeyDrivers: stringList(Array.isArray(transport.journey_drivers)
      ? transport.journey_drivers
      : hasScopedDrivers ? [] : transport.drivers),
    journeyPassengers: stringList(Array.isArray(transport.journey_passengers)
      ? transport.journey_passengers
      : hasScopedPassengers ? [] : transport.passengers),
  };
}

function peopleSection(icon, singular, plural, names) {
  const label = names.length === 1 ? singular : plural;
  return `${icon} ${label}:\n${names.join('\n')}`;
}

export function groundTravelRoleDescription(people = {}) {
  const personnel = stringList(people.personnel);
  const drivers = stringList(people.drivers);
  const passengers = stringList(people.passengers);
  const journeyDrivers = stringList(people.journeyDrivers);
  const journeyPassengers = stringList(people.journeyPassengers);
  const otherJourneyDrivers = journeyDrivers.filter((name) => !drivers.includes(name));
  const otherJourneyPassengers = journeyPassengers.filter((name) => !passengers.includes(name));
  const sections = [];

  if (drivers.length) {
    sections.push(peopleSection('🚘', 'Driver', 'Drivers', drivers));
    if (otherJourneyDrivers.length) {
      sections.push(peopleSection('🚘', 'Other Trip Driver', 'Other Trip Drivers', otherJourneyDrivers));
    }
  } else if (journeyDrivers.length) {
    sections.push(peopleSection('🚘', 'Trip Driver', 'Trip Drivers', journeyDrivers));
  }

  if (passengers.length) {
    sections.push(peopleSection('🧳', 'Passenger', 'Passengers', passengers));
    if (otherJourneyPassengers.length) {
      sections.push(peopleSection('🧳', 'Other Trip Passenger', 'Other Trip Passengers', otherJourneyPassengers));
    }
  } else if (journeyPassengers.length) {
    sections.push(peopleSection('🧳', 'Trip Passenger', 'Trip Passengers', journeyPassengers));
  } else if (drivers.length || journeyDrivers.length) {
    sections.push('🧳 Passengers:\nNone assigned at this stop');
  }

  if (!sections.length && personnel.length) {
    sections.push(peopleSection('👥', 'Personnel', 'Personnel', personnel));
  }

  return sections.join('\n\n');
}

export function groundStopCalendarDescription(notes, details) {
  const stopNotes = typeof notes === 'string' ? notes.trim() : '';
  if (!stopNotes) return details;
  return [stopNotes, details === 'Ground transportation details' ? '' : details]
    .filter(Boolean)
    .join('\n\n');
}

export function groundStopPersonnelDescription(transport = {}) {
  const participants = Array.isArray(transport.stop_participants)
    ? transport.stop_participants : [];
  const lines = [];
  for (const [role, heading] of [['driver', 'Drivers'], ['passenger', 'Passengers']]) {
    const names = participants.filter((person) => person?.role === role
      && typeof person.name === 'string' && person.name.trim())
      .map((person) => `- ${person.name.trim()}`);
    if (names.length) lines.push(`${heading}:\n${names.join('\n')}`);
  }
  const notes = typeof transport.notes === 'string' ? transport.notes.trim() : '';
  if (notes) lines.push(`Stop Notes:\n${notes}`);
  const url = calendarPersonnelTravelDescriptionUrl(
    transport.transportation_web_url, transport.transportation_url
  );
  if (url) lines.push(`Ground Details: ${url}`);
  return lines.join('\n\n') || 'Ground transportation details';
}
