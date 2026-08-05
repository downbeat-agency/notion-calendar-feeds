import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assertCalendarRehearsalSnapshotExpectedIds,
  buildCalendarRehearsalMembershipSnapshot,
  calendarRehearsalMembershipMap,
  personalCalendarRecentDateStart,
  rehearsalDateFromFormulaProperty,
  rehearsalIdFromCalendarFormulaRow,
} from './calendar-rehearsal-membership.js';

const personOne = '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2';
const personTwo = '34139e4a-65a9-8080-b15b-e103497ea4a5';
const rehearsalOne = '30b39e4a-65a9-8021-8287-fd5a5f0d206f';
const rehearsalTwo = '39f39e4a-65a9-807b-8cd6-f86143aea5ba';

function relation(ids, hasMore = false) {
  return { relation: ids.map((id) => ({ id })), has_more: hasMore };
}

function row({
  personnel = [personOne],
  rehearsals = [rehearsalOne],
  date = '@August 6, 2026 10:00 AM (PDT) → 12:00 PM',
  status = 'Confirmed',
  rehearsalStatus = null,
} = {}) {
  return {
    properties: {
      Personnel: relation(personnel),
      Rehearsals: relation(rehearsals),
      'Rehearsal Date/Time': { formula: { type: 'string', string: date } },
      Status: { status: { name: status } },
      'Rehearsal Status': { select: rehearsalStatus ? { name: rehearsalStatus } : null },
    },
  };
}

test('rehearsal witness keeps recent active memberships and retains empty people', () => {
  const snapshot = buildCalendarRehearsalMembershipSnapshot([
    row(),
    row(),
    row({ rehearsals: [rehearsalTwo], status: 'Unconfirmed' }),
    row({ rehearsals: [rehearsalTwo], rehearsalStatus: 'Declined' }),
    row({ rehearsals: [rehearsalTwo], date: '@July 1, 2026 10:00 AM → 12:00 PM' }),
  ], [personOne, personTwo], { dateFrom: '2026-07-21' });

  const byPerson = calendarRehearsalMembershipMap(snapshot);
  assert.equal(snapshot.sourceRowCount, 5);
  assert.equal(snapshot.membershipCount, 2);
  assert.deepEqual(byPerson.get(personOne), [rehearsalOne, rehearsalTwo]);
  assert.deepEqual(byPerson.get(personTwo), []);
});

test('rehearsal witness fails closed on truncated relations and invalid input', () => {
  const truncated = row();
  truncated.properties.Rehearsals.has_more = true;
  assert.throws(
    () => buildCalendarRehearsalMembershipSnapshot([truncated], [], { dateFrom: '2026-07-21' }),
    (error) => error.code === 'NOTION_CALENDAR_REHEARSAL_RELATION_TRUNCATED'
  );
  assert.throws(
    () => buildCalendarRehearsalMembershipSnapshot([], [], { dateFrom: 'not-a-date' }),
    (error) => error.code === 'NOTION_CALENDAR_REHEARSAL_MEMBERSHIP_INVALID'
  );
});

test('rehearsal formula dates and app links expose stable rehearsal identity', () => {
  assert.equal(
    rehearsalDateFromFormulaProperty({
      formula: { string: '@December 31, 2026 10:00 AM (PST) → 12:00 PM' },
    }),
    '2026-12-31'
  );
  assert.equal(
    rehearsalIdFromCalendarFormulaRow({
      app_link: `https://app.downbeat.agency/rehearsal/${rehearsalOne}`,
    }),
    rehearsalOne
  );
  assert.equal(
    rehearsalIdFromCalendarFormulaRow({
      app_link: `https://example.com/rehearsal/${rehearsalOne}`,
    }),
    null
  );
});

test('rehearsal formula completeness rejects a transient empty snapshot', () => {
  const rows = [{ app_link: `https://app.downbeat.agency/rehearsal/${rehearsalOne}` }];
  assert.deepEqual(
    assertCalendarRehearsalSnapshotExpectedIds(rows, [rehearsalOne]),
    { expectedCount: 1, actualCount: 1 }
  );
  assert.throws(
    () => assertCalendarRehearsalSnapshotExpectedIds([], [rehearsalOne]),
    (error) => error.code === 'NOTION_CALENDAR_REHEARSAL_SNAPSHOT_INCOMPLETE'
  );
});

test('personnel rehearsal lookback is a Pacific floating date', () => {
  assert.equal(
    personalCalendarRecentDateStart(new Date('2026-08-05T06:30:00.000Z')),
    '2026-07-21'
  );
  assert.equal(
    personalCalendarRecentDateStart(new Date('2026-08-05T08:30:00.000Z')),
    '2026-07-22'
  );
});
