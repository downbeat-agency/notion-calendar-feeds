const INACTIVE_STATUSES = new Set([
  'archived',
  'cancelled',
  'canceled',
  'deleted',
  'declined',
  'needed',
  'void',
  'voided',
]);

function normalizeNotionPageId(value) {
  const compact = String(value || '').trim().toLowerCase().replaceAll('-', '');
  if (!/^[0-9a-f]{32}$/u.test(compact)) return null;
  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20),
  ].join('-');
}

function relationIds(property, label) {
  if (property?.has_more) {
    const error = new Error(`${label} relation was truncated in the Notion rehearsal witness.`);
    error.code = 'NOTION_CALENDAR_REHEARSAL_RELATION_TRUNCATED';
    throw error;
  }
  return (Array.isArray(property?.relation) ? property.relation : [])
    .map((relation) => normalizeNotionPageId(relation?.id))
    .filter(Boolean);
}

function propertyStatus(property) {
  return String(property?.status?.name || property?.select?.name || '').trim().toLowerCase();
}

export function rehearsalDateFromFormulaProperty(property) {
  const direct = String(property?.formula?.date?.start || '').match(/^\d{4}-\d{2}-\d{2}/u)?.[0];
  if (direct) return direct;
  const match = String(property?.formula?.string || '').match(
    /^@([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})/u
  );
  if (!match) return null;
  const month = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
  ].indexOf(match[1].toLowerCase()) + 1;
  return month > 0
    ? `${match[3]}-${String(month).padStart(2, '0')}-${String(match[2]).padStart(2, '0')}`
    : null;
}

export function personalCalendarRecentDateStart(
  now = new Date(),
  timeZone = 'America/Los_Angeles',
  lookbackDays = 14
) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const part = (type) => parts.find((entry) => entry.type === type)?.value;
  const date = new Date(Date.UTC(
    Number(part('year')),
    Number(part('month')) - 1,
    Number(part('day')) - Math.max(0, Number(lookbackDays) || 0)
  ));
  return date.toISOString().slice(0, 10);
}

export function buildCalendarRehearsalMembershipSnapshot(
  rows = [],
  personnelIds = [],
  { dateFrom = personalCalendarRecentDateStart() } = {}
) {
  if (!Array.isArray(rows) || !Array.isArray(personnelIds)) {
    const error = new Error('Notion rehearsal membership witness input is invalid.');
    error.code = 'NOTION_CALENDAR_REHEARSAL_MEMBERSHIP_INVALID';
    throw error;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/u.test(String(dateFrom || ''))) {
    const error = new Error('Notion rehearsal membership witness date is invalid.');
    error.code = 'NOTION_CALENDAR_REHEARSAL_MEMBERSHIP_INVALID';
    throw error;
  }

  const byPerson = new Map();
  for (const rawPersonId of personnelIds) {
    const personId = normalizeNotionPageId(rawPersonId);
    if (personId) byPerson.set(personId, new Set());
  }

  for (const row of rows) {
    const properties = row?.properties || {};
    const rehearsalDate = rehearsalDateFromFormulaProperty(properties['Rehearsal Date/Time']);
    const eventStatus = propertyStatus(properties.Status);
    const rehearsalStatus = propertyStatus(properties['Rehearsal Status']);
    if (!rehearsalDate || rehearsalDate < dateFrom) continue;
    if (INACTIVE_STATUSES.has(eventStatus) || INACTIVE_STATUSES.has(rehearsalStatus)) continue;
    const rowPersonnelIds = relationIds(properties.Personnel, 'Personnel');
    const rehearsalIds = relationIds(properties.Rehearsals, 'Rehearsals');
    for (const personId of rowPersonnelIds) {
      if (!byPerson.has(personId)) byPerson.set(personId, new Set());
      for (const rehearsalId of rehearsalIds) byPerson.get(personId).add(rehearsalId);
    }
  }

  const memberships = [...byPerson.entries()]
    .map(([personId, rehearsalIds]) => ({
      personId,
      rehearsalIds: [...rehearsalIds].sort(),
    }))
    .sort((left, right) => left.personId.localeCompare(right.personId));
  return {
    sourceRowCount: rows.length,
    membershipCount: memberships.reduce(
      (sum, entry) => sum + entry.rehearsalIds.length,
      0
    ),
    dateFrom,
    memberships,
  };
}

export function calendarRehearsalMembershipMap(snapshot) {
  if (!Array.isArray(snapshot?.memberships)) {
    const error = new Error('Notion rehearsal membership witness snapshot is invalid.');
    error.code = 'NOTION_CALENDAR_REHEARSAL_MEMBERSHIP_INVALID';
    throw error;
  }
  return new Map(snapshot.memberships.map(
    (entry) => [entry.personId, [...entry.rehearsalIds]]
  ));
}

export function rehearsalIdFromCalendarFormulaRow(row = {}) {
  try {
    const parsed = new URL(String(row?.app_link || ''));
    if (parsed.hostname.toLowerCase() !== 'app.downbeat.agency') return null;
    const match = parsed.pathname.match(
      /^\/rehearsal\/([0-9a-f]{32}|[0-9a-f-]{36})(?:\/|$)/iu
    );
    return normalizeNotionPageId(match?.[1]);
  } catch {
    return null;
  }
}

export function assertCalendarRehearsalSnapshotExpectedIds(rows = [], expectedIds = []) {
  const actual = new Set((Array.isArray(rows) ? rows : [])
    .map(rehearsalIdFromCalendarFormulaRow)
    .filter(Boolean));
  const expected = [...new Set((Array.isArray(expectedIds) ? expectedIds : [])
    .map(normalizeNotionPageId)
    .filter(Boolean))];
  const missing = expected.filter((id) => !actual.has(id));
  if (missing.length > 0) {
    const error = new Error(
      `Notion Calendar Data rehearsal formula is missing ${missing.length} witnessed rehearsal(s).`
    );
    error.code = 'NOTION_CALENDAR_REHEARSAL_SNAPSHOT_INCOMPLETE';
    error.details = { expectedCount: expected.length, actualCount: actual.size, missingCount: missing.length };
    throw error;
  }
  return { expectedCount: expected.length, actualCount: actual.size };
}
