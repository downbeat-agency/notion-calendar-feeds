const DEFAULT_EVENT_HUB_BASE_URL = 'https://music.downbeat.agency';
const DEFAULT_APP_BASE_URL = 'https://app.downbeat.agency';
const EVENT_DETAILS_TIME_ZONE = 'America/Los_Angeles';

function clean(value, limit = 2_000) {
  return String(value ?? '').trim().slice(0, limit);
}

function normalizedNotionPageId(value) {
  const compact = clean(value, 100).toLowerCase().replaceAll('-', '');
  if (!/^[0-9a-f]{32}$/u.test(compact)) return '';
  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20),
  ].join('-');
}

function eventSelectorFromOccurrenceKey(value) {
  const match = clean(value, 200).match(/^event:([0-9a-f-]{32,36})$/iu);
  if (!match) return '';
  const candidate = match[1];
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(candidate)) {
    return candidate.toLowerCase();
  }
  return normalizedNotionPageId(candidate);
}

function eventSelectorFromComparisonIdentity(value) {
  const match = clean(value, 200).match(/^event:([0-9a-f-]{32,36})$/iu);
  return match ? normalizedNotionPageId(match[1]) : '';
}

function eventSelectorFromNotionUrl(value) {
  const candidate = clean(value);
  let pathname = candidate.split(/[?#]/u)[0];
  try {
    const parsed = new URL(candidate);
    const host = parsed.hostname.toLowerCase();
    if (host !== 'notion.so' && !host.endsWith('.notion.so') && host !== 'notion.com' && !host.endsWith('.notion.com')) {
      return '';
    }
    pathname = parsed.pathname;
  } catch {
    // Legacy formula values are expected to be URLs, but a path-only value can
    // still be resolved safely because query/hash fragments were removed above.
  }
  const matches = pathname.match(/[0-9a-f]{32}/giu) || [];
  return normalizedNotionPageId(matches.at(-1));
}

function normalizedEventHubBaseUrl(value) {
  const candidate = clean(value, 500) || DEFAULT_EVENT_HUB_BASE_URL;
  try {
    const parsed = new URL(candidate);
    if (!['http:', 'https:'].includes(parsed.protocol)) return DEFAULT_EVENT_HUB_BASE_URL;
    return `${parsed.origin}${parsed.pathname.replace(/\/+$/u, '')}`;
  } catch {
    return DEFAULT_EVENT_HUB_BASE_URL;
  }
}

export function calendarEventHubUrl(event = {}, options = {}) {
  const selector = eventSelectorFromOccurrenceKey(event.occurrence_key || event.occurrenceKey)
    || eventSelectorFromComparisonIdentity(
      event._comparison_identity || event.comparisonIdentity
    )
    || eventSelectorFromNotionUrl(event.notion_url);
  if (!selector) return '';
  const baseUrl = normalizedEventHubBaseUrl(options.baseUrl);
  return `${baseUrl}/events/${encodeURIComponent(selector)}`;
}

function validHttpUrl(value) {
  const candidate = clean(value, 2_000);
  if (!candidate) return '';
  try {
    const parsed = new URL(candidate);
    return ['http:', 'https:'].includes(parsed.protocol) ? parsed.toString() : '';
  } catch {
    return '';
  }
}

export function calendarAppUrl(event = {}, options = {}) {
  const explicit = validHttpUrl(event.appUrl || event.app_url);
  if (explicit) return explicit;
  const selector = eventSelectorFromOccurrenceKey(event.occurrence_key || event.occurrenceKey)
    || eventSelectorFromComparisonIdentity(
      event._comparison_identity || event.comparisonIdentity
    )
    || eventSelectorFromNotionUrl(event.notion_url);
  if (!selector) return '';
  const baseUrl = normalizedEventHubBaseUrl(options.baseUrl || DEFAULT_APP_BASE_URL);
  return `${baseUrl}/events/${encodeURIComponent(selector)}`;
}

function normalizedCalendarCopy(value = '') {
  return String(value ?? '')
    .replace(/\\r\\n|\\n|\\r/gu, '\n')
    .replace(/\r\n?/gu, '\n')
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .replace(/\n{3,}/gu, '\n\n')
    .trim();
}

export function calendarDescriptionWithoutTimelineLink(value = '') {
  return normalizedCalendarCopy(value)
    .replace(
      /(^|\n)[\t ]*Timeline(?: Link)?:[\t ]*https?:\/\/[^\s\r\n]+[\t ]*(?=\r?\n|$)/giu,
      '$1'
    )
    .replace(/\n{3,}/gu, '\n\n')
    .trim();
}

const MAIN_EVENT_HEADING_DEFINITIONS = Object.freeze({
  'general info': { label: 'EVENT DETAILS', kind: 'group' },
  'event details': { label: 'EVENT DETAILS', kind: 'group' },
  'parking and load in': { label: 'Load In / Parking', kind: 'field' },
  'load in / parking': { label: 'Load In / Parking', kind: 'field' },
  'load-in / parking': { label: 'Load In / Parking', kind: 'field' },
  'dress code': { label: 'Dress Code', kind: 'field' },
  power: { label: 'Power', kind: 'field' },
  'green room': { label: 'Green Room', kind: 'field' },
  'event notes': { label: 'Event Notes', kind: 'field' },
  'day of contact for band': { label: 'Day-of Contact', kind: 'field' },
  'day-of contact for band': { label: 'Day-of Contact', kind: 'field' },
  'day of contact': { label: 'Day-of Contact', kind: 'field' },
  'day-of contact': { label: 'Day-of Contact', kind: 'field' },
  contracted: { label: 'CONTRACTED', kind: 'group' },
  'additional information': { label: 'ADDITIONAL INFORMATION', kind: 'group' },
  links: { label: 'LINKS', kind: 'group' },
});

function mainEventHeading(value) {
  const normalized = String(value || '')
    .trim()
    .replace(/^[📋]+\s*/u, '')
    .replace(/:\s*$/u, '')
    .toLowerCase();
  return MAIN_EVENT_HEADING_DEFINITIONS[normalized] || null;
}

function mainEventHeadingParts(value) {
  const source = String(value || '').trim().replace(/^[📋]+\s*/u, '');
  const inlineMatch = source.match(/^([^:]{2,80}):\s*(.*)$/u);
  if (inlineMatch) {
    const heading = MAIN_EVENT_HEADING_DEFINITIONS[inlineMatch[1].trim().toLowerCase()];
    if (heading) return { heading, inline: inlineMatch[2].trim() };
  }
  const heading = mainEventHeading(source);
  return heading ? { heading, inline: '' } : null;
}

function labeledUrl(line, labels) {
  const escapedLabels = labels.join('|').replaceAll(' ', '\\s+');
  const match = String(line || '').trim().match(
    new RegExp(`^(?:${escapedLabels}):\\s*(https?:\\/\\/\\S+)\\s*$`, 'iu')
  );
  return match?.[1] || '';
}

function eventHubUrlFromEmbeddedUrl(value, options = {}) {
  const candidate = clean(value);
  if (!candidate) return '';
  try {
    const parsed = new URL(candidate);
    const match = parsed.pathname.match(/^\/events?\/([0-9a-f-]{32,36})(?:\/|$)/iu);
    if (!match) return '';
    const selector = /^[0-9a-f]{32}$/iu.test(match[1])
      ? normalizedNotionPageId(match[1])
      : match[1].toLowerCase();
    if (!selector) return '';
    return `${normalizedEventHubBaseUrl(options.baseUrl)}/events/${encodeURIComponent(selector)}`;
  } catch {
    return '';
  }
}

function tidyContractTime(value) {
  return String(value || '')
    .replace(/(\d)(am|pm)\b/giu, '$1 $2')
    .replace(/\s*[-–]\s*/u, '–')
    .replace(/\b(am|pm)\b/giu, (match) => match.toUpperCase())
    .trim();
}

function formattedContractLine(rawLine) {
  const line = String(rawLine || '').trim();
  if (!line || /^[•*-]\s/u.test(line) || /^\s{2}/u.test(rawLine)) return [rawLine.trimEnd()];
  const match = line.match(/^([^:]{2,50}):\s*(.+)$/u);
  if (!match) return [line];
  const trailingTime = match[2].match(
    /(?:^|\s)(\d{1,2}:\d{2}\s*(?:am|pm)?\s*[-–]\s*\d{1,2}:\d{2}\s*(?:am|pm)?(?:\s*\([^)]*\))?)\s*$/iu
  );
  const service = trailingTime
    ? match[2].slice(0, trailingTime.index).trim()
    : match[2].trim();
  const first = `• ${match[1].trim()}${trailingTime ? ` · ${tidyContractTime(trailingTime[1])}` : ''}`;
  return service ? [first, `  ${service}`] : [first];
}

function nonEmptyMainEventLines(lines = []) {
  return lines.filter((line, index) => {
    const heading = mainEventHeading(line);
    if (!heading) return true;
    const nextIndex = lines.findIndex((candidate, candidateIndex) => (
      candidateIndex > index && String(candidate || '').trim()
    ));
    if (nextIndex < 0) return false;
    const nextHeading = mainEventHeading(lines[nextIndex]);
    if (heading.kind === 'field') return !nextHeading;
    return !nextHeading || nextHeading.kind === 'field';
  });
}

function spacedMainEventBody(lines = []) {
  const output = [];
  lines.forEach((rawLine) => {
    const line = String(rawLine || '').trimEnd();
    const heading = mainEventHeading(line);
    if (heading) {
      if (output.length && output.at(-1) !== '') output.push('');
      if (output.at(-2) !== heading.label) output.push(heading.label);
      if (heading.kind === 'group') output.push('');
      return;
    }
    if (!line.trim()) {
      if (output.length && output.at(-1) !== '') output.push('');
      return;
    }
    if (/^(?:n\/?a|none|null|-+)$/iu.test(line.trim())) return;
    output.push(line);
  });
  return output.join('\n').replace(/\n{3,}/gu, '\n\n').trim();
}

function cleanMainEventDescription(value = '') {
  let pcoUrl = '';
  let embeddedEventUrl = '';
  let embeddedAppUrl = '';
  let embeddedDetailsUpdated = '';
  let inContracted = false;
  const lines = [];
  calendarDescriptionWithoutTimelineLink(value).split('\n').forEach((rawLine) => {
    const line = rawLine.trim();
    const nextPcoUrl = labeledUrl(line, ['PCO Link', 'PCO Plan']);
    if (nextPcoUrl) {
      pcoUrl ||= nextPcoUrl;
      return;
    }
    const nextEventUrl = labeledUrl(line, ['Event Link']);
    if (nextEventUrl) {
      embeddedEventUrl ||= nextEventUrl;
      return;
    }
    const nextAppUrl = labeledUrl(line, ['App Link']);
    if (nextAppUrl) {
      embeddedAppUrl ||= nextAppUrl;
      return;
    }
    if (/^Notion\s+Link:\s*https?:\/\//iu.test(line)) return;
    const updatedMatch = line.match(
      /^(?:(?:General\s+)?Notes\s+Updated|Event\s+Details\s+Updated):\s*(.*)$/iu
    );
    if (updatedMatch) {
      embeddedDetailsUpdated ||= updatedMatch[1].trim();
      return;
    }
    if (/^(?:n\/?a|none|null|-+)$/iu.test(line)) return;
    const headingParts = mainEventHeadingParts(line);
    const heading = headingParts?.heading || null;
    if (heading?.label === 'LINKS') {
      inContracted = false;
      return;
    }
    if (heading) {
      inContracted = heading.label === 'CONTRACTED';
      lines.push(heading.label);
      if (headingParts.inline) {
        if (inContracted) lines.push(...formattedContractLine(headingParts.inline));
        else lines.push(headingParts.inline);
      }
      return;
    }
    if (inContracted) {
      lines.push(...formattedContractLine(rawLine));
      return;
    }
    lines.push(rawLine);
  });
  return {
    body: spacedMainEventBody(nonEmptyMainEventLines(lines)),
    embeddedAppUrl,
    embeddedDetailsUpdated,
    embeddedEventUrl,
    pcoUrl,
  };
}

function formattedDateOnly(value) {
  const match = clean(value, 100).match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  if (!match) return '';
  const date = new Date(`${match[1]}-${match[2]}-${match[3]}T12:00:00Z`);
  if (!Number.isFinite(date.getTime())) return '';
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  }).format(date);
}

function formattedExactUpdate(value) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return '';
  return new Intl.DateTimeFormat('en-US', {
    timeZone: EVENT_DETAILS_TIME_ZONE,
    month: 'long',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  }).format(date);
}

function normalizedLegacyUpdateLabel(value) {
  return clean(value, 300)
    .replace(/^@\s*/u, '')
    .replace(/\s+\((P[DS]T)\)$/u, ' $1');
}

export function calendarEventDetailsUpdatedLabel(event = {}, legacyFallback = '') {
  const value = clean(event.eventDetailsUpdatedAt || event.event_details_updated_at, 100);
  const precision = clean(
    event.eventDetailsUpdatedPrecision || event.event_details_updated_precision,
    20
  ).toLowerCase();
  if (value) {
    const dateOnly = precision === 'date' || /^\d{4}-\d{2}-\d{2}$/u.test(value);
    const formatted = dateOnly ? formattedDateOnly(value) : formattedExactUpdate(value);
    if (formatted) return formatted;
  }
  return normalizedLegacyUpdateLabel(legacyFallback);
}

export function calendarEventWithEventHubLink(event = {}) {
  if (clean(event.type, 100) !== 'main_event' || typeof event.description !== 'string') {
    return event;
  }
  const normalizedDescription = calendarDescriptionWithoutTimelineLink(event.description);
  const notionLink = normalizedDescription.match(/(^|\n)Notion Link:\s*(https?:\/\/[^\s\r\n]+)/u);
  const cleaned = cleanMainEventDescription(normalizedDescription);
  const eventHubUrl = calendarEventHubUrl(event)
    || eventHubUrlFromEmbeddedUrl(cleaned.embeddedEventUrl)
    || eventHubUrlFromEmbeddedUrl(cleaned.embeddedAppUrl)
    || (notionLink ? calendarEventHubUrl({ notion_url: notionLink[2] }) : '');
  const appUrl = calendarAppUrl(event)
    || (notionLink ? calendarAppUrl({ notion_url: notionLink[2] }) : '')
    || validHttpUrl(cleaned.embeddedAppUrl);
  const detailsUpdated = calendarEventDetailsUpdatedLabel(
    event,
    cleaned.embeddedDetailsUpdated
  );
  const linkLines = [
    eventHubUrl ? `Event Link: ${eventHubUrl}` : '',
    appUrl ? `App Link: ${appUrl}` : '',
    cleaned.pcoUrl ? `PCO Plan: ${cleaned.pcoUrl}` : '',
  ].filter(Boolean);
  const description = [
    cleaned.body,
    linkLines.length ? `LINKS\n\n${linkLines.join('\n')}` : '',
    detailsUpdated ? `Event Details Updated: ${detailsUpdated}` : '',
  ].filter(Boolean).join('\n\n');
  const decorated = {
    ...event,
    description,
    ...(eventHubUrl ? { url: eventHubUrl } : {}),
  };
  delete decorated.appUrl;
  delete decorated.app_url;
  delete decorated.eventDetailsUpdatedAt;
  delete decorated.event_details_updated_at;
  delete decorated.eventDetailsUpdatedPrecision;
  delete decorated.event_details_updated_precision;
  return decorated;
}

export function calendarTravelLinkLabel(value, travelType = '') {
  const url = clean(value);
  if (!url) return '';
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    if (host === 'notion.so' || host.endsWith('.notion.so') || host === 'notion.com' || host.endsWith('.notion.com')) {
      return 'Notion Link';
    }
    const tab = clean(parsed.searchParams.get('travel'), 40).toLowerCase();
    const isEventTravelLink = /^\/events\/[^/]+/u.test(parsed.pathname)
      && parsed.searchParams.get('section') === 'travel';
    if (!isEventTravelLink) return 'Travel Details';
    if (tab === 'hotels' || travelType === 'hotels') return 'Hotel Details';
    if (tab === 'flights' || travelType === 'flights') return 'Flight Details';
    if (tab === 'ground' || travelType === 'ground') return 'Ground Details';
    return 'Travel Details';
  } catch {
    return 'Travel Details';
  }
}

export { DEFAULT_APP_BASE_URL, DEFAULT_EVENT_HUB_BASE_URL };
