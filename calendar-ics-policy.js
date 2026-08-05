const GOOGLE_CALENDAR_TIMEZONE = 'America/Los_Angeles';
const GOOGLE_VTIMEZONE_BLOCK = [
  'BEGIN:VTIMEZONE',
  `TZID:${GOOGLE_CALENDAR_TIMEZONE}`,
  `X-LIC-LOCATION:${GOOGLE_CALENDAR_TIMEZONE}`,
  'BEGIN:DAYLIGHT',
  'TZOFFSETFROM:-0800',
  'TZOFFSETTO:-0700',
  'TZNAME:PDT',
  'DTSTART:19700308T020000',
  'RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU',
  'END:DAYLIGHT',
  'BEGIN:STANDARD',
  'TZOFFSETFROM:-0700',
  'TZOFFSETTO:-0800',
  'TZNAME:PST',
  'DTSTART:19701101T020000',
  'RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU',
  'END:STANDARD',
  'END:VTIMEZONE',
].join('\r\n');

export function configuredCalendarTimeMode(env = process.env) {
  const value = String(env.CALENDAR_TIME_MODE || 'floating').trim().toLowerCase();
  if (!['floating', 'legacy-la'].includes(value)) {
    throw new Error('CALENDAR_TIME_MODE must be floating or legacy-la.');
  }
  return value;
}

function addLegacyLosAngelesMetadata(icsData) {
  if (typeof icsData !== 'string' || icsData.includes('X-WR-TIMEZONE:')) return icsData;
  return icsData.replace(
    /(VERSION:2\.0\r?\n)/u,
    `$1X-WR-TIMEZONE:${GOOGLE_CALENDAR_TIMEZONE}\r\n`
  );
}

export function serializeCalendarWithTimePolicy(calendar, options = {}) {
  if (!calendar) return '';
  const mode = options.mode || configuredCalendarTimeMode(options.env);
  const icsData = calendar.toString();
  return mode === 'floating' ? icsData : addLegacyLosAngelesMetadata(icsData);
}

export function serializeGoogleCalendarWithTimePolicy(calendar, options = {}) {
  if (!calendar) return '';
  const mode = options.mode || configuredCalendarTimeMode(options.env);
  if (mode === 'floating') return calendar.toString();

  let icsData = addLegacyLosAngelesMetadata(calendar.toString())
    .replace(/\r?\nDTSTART:(\d{8}T\d{6})/gu, `\r\nDTSTART;TZID=${GOOGLE_CALENDAR_TIMEZONE}:$1`)
    .replace(/\r?\nDTEND:(\d{8}T\d{6})/gu, `\r\nDTEND;TZID=${GOOGLE_CALENDAR_TIMEZONE}:$1`);
  if (!icsData.includes('BEGIN:VTIMEZONE')) {
    icsData = icsData.replace(
      /(X-WR-TIMEZONE:[^\r\n]+\r?\n)/u,
      `$1${GOOGLE_VTIMEZONE_BLOCK}\r\n`
    );
  }
  return icsData;
}

export { GOOGLE_CALENDAR_TIMEZONE };
