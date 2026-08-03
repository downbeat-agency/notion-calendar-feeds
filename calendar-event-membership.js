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
    const error = new Error(`${label} relation was truncated in the Notion membership witness.`);
    error.code = 'NOTION_CALENDAR_MEMBERSHIP_RELATION_TRUNCATED';
    throw error;
  }
  return (Array.isArray(property?.relation) ? property.relation : [])
    .map((relation) => normalizeNotionPageId(relation?.id))
    .filter(Boolean);
}

export function buildCalendarEventMembershipSnapshot(rows = [], personnelIds = []) {
  if (!Array.isArray(rows) || !Array.isArray(personnelIds)) {
    const error = new Error('Notion calendar membership witness input is invalid.');
    error.code = 'NOTION_CALENDAR_MEMBERSHIP_INVALID';
    throw error;
  }

  const byPerson = new Map();
  for (const rawPersonId of personnelIds) {
    const personId = normalizeNotionPageId(rawPersonId);
    if (personId) byPerson.set(personId, new Set());
  }

  for (const row of rows) {
    const properties = row?.properties || {};
    const rowPersonnelIds = relationIds(properties.Personnel, 'Personnel');
    const eventIds = relationIds(properties.Event, 'Event');
    for (const personId of rowPersonnelIds) {
      if (!byPerson.has(personId)) byPerson.set(personId, new Set());
      for (const eventId of eventIds) byPerson.get(personId).add(eventId);
    }
  }

  const memberships = [...byPerson.entries()]
    .map(([personId, eventIds]) => ({
      personId,
      eventIds: [...eventIds].sort(),
    }))
    .sort((left, right) => left.personId.localeCompare(right.personId));

  return {
    sourceRowCount: rows.length,
    membershipCount: memberships.reduce((sum, entry) => sum + entry.eventIds.length, 0),
    memberships,
  };
}

export function calendarEventMembershipMap(snapshot) {
  if (!Array.isArray(snapshot?.memberships)) {
    const error = new Error('Notion calendar membership witness snapshot is invalid.');
    error.code = 'NOTION_CALENDAR_MEMBERSHIP_INVALID';
    throw error;
  }
  return new Map(snapshot.memberships.map((entry) => [entry.personId, [...entry.eventIds]]));
}
