import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildCalendarEventMembershipSnapshot,
  calendarEventMembershipMap,
} from './calendar-event-membership.js';

const personOne = '29aaf64b-cf86-4d3c-b117-3a58cf6c76f2';
const personTwo = '34139e4a-65a9-8080-b15b-e103497ea4a5';
const eventOne = '30b39e4a-65a9-8021-8287-fd5a5f0d206f';
const eventTwo = '39f39e4a-65a9-807b-8cd6-f86143aea5ba';

function relation(ids, hasMore = false) {
  return { relation: ids.map((id) => ({ id })), has_more: hasMore };
}

test('membership snapshot groups unique events by personnel and retains empty people', () => {
  const snapshot = buildCalendarEventMembershipSnapshot([
    { properties: { Personnel: relation([personOne]), Event: relation([eventOne]) } },
    { properties: { Personnel: relation([personOne]), Event: relation([eventOne]) } },
    { properties: { Personnel: relation([personOne, personTwo]), Event: relation([eventTwo]) } },
  ], [personOne, 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa']);

  const byPerson = calendarEventMembershipMap(snapshot);
  assert.equal(snapshot.sourceRowCount, 3);
  assert.equal(snapshot.membershipCount, 3);
  assert.deepEqual(byPerson.get(personOne), [eventOne, eventTwo]);
  assert.deepEqual(byPerson.get(personTwo), [eventTwo]);
  assert.deepEqual(byPerson.get('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'), []);
});

test('membership snapshot fails closed when a relation is truncated', () => {
  assert.throws(
    () => buildCalendarEventMembershipSnapshot([
      { properties: { Personnel: relation([personOne], true), Event: relation([eventOne]) } },
    ]),
    (error) => error.code === 'NOTION_CALENDAR_MEMBERSHIP_RELATION_TRUNCATED'
  );
});
