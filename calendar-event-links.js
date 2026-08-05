const DEFAULT_EVENT_HUB_BASE_URL = 'https://music.downbeat.agency';

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
  const selector = eventSelectorFromOccurrenceKey(event.occurrence_key)
    || eventSelectorFromComparisonIdentity(event._comparison_identity)
    || eventSelectorFromNotionUrl(event.notion_url);
  if (!selector) return '';
  const baseUrl = normalizedEventHubBaseUrl(options.baseUrl);
  return `${baseUrl}/events/${encodeURIComponent(selector)}`;
}

export function calendarDescriptionWithoutTimelineLink(value = '') {
  return String(value)
    .replace(
      /(^|\n)[\t ]*Timeline(?: Link)?:[\t ]*https?:\/\/[^\s\r\n]+[\t ]*(?=\r?\n|$)/giu,
      '$1'
    )
    .replace(/\n{3,}/gu, '\n\n')
    .trim();
}

export function calendarEventWithEventHubLink(event = {}) {
  if (clean(event.type, 100) !== 'main_event' || typeof event.description !== 'string') {
    return event;
  }
  const description = calendarDescriptionWithoutTimelineLink(event.description);
  const notionLink = description.match(/(^|\n)Notion Link:\s*(https?:\/\/[^\s\r\n]+)/u);
  if (!notionLink) {
    return description === event.description ? event : { ...event, description };
  }
  const eventHubUrl = calendarEventHubUrl({ notion_url: notionLink[2] });
  if (!eventHubUrl) {
    return description === event.description ? event : { ...event, description };
  }
  return {
    ...event,
    description: description.replace(
      notionLink[0],
      `${notionLink[1]}Event Link: ${eventHubUrl}`
    ),
  };
}

export { DEFAULT_EVENT_HUB_BASE_URL };
