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
