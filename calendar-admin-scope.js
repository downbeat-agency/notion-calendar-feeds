const ADMIN_CALENDAR_EVENT_TYPES = new Set([
  'main_event',
  'rehearsal',
]);

export function adminCalendarEventsOnly(events) {
  if (!Array.isArray(events)) return [];
  return events.filter((event) => ADMIN_CALENDAR_EVENT_TYPES.has(event?.type));
}
