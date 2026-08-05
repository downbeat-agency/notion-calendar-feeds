import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  calendarDescriptionWithoutTimelineLink,
  calendarEventHubUrl,
  calendarEventWithEventHubLink,
} from './calendar-event-links.js';

test('legacy Notion event links become authenticated Event Hub deep links', () => {
  assert.equal(
    calendarEventHubUrl({
      notion_url: 'https://www.notion.so/2b639e4a65a9800aaa682f4e2eed5215',
    }),
    'https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215'
  );
});

test('Notion database-view query IDs cannot replace the event page identity', () => {
  assert.equal(
    calendarEventHubUrl({
      notion_url: 'https://app.notion.com/p/downbeat/2b639e4a65a9800aaa682f4e2eed5215?v=82ddac10854f4ec2a578406eb7192c5d',
    }),
    'https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215'
  );
});

test('Postgres occurrence identity becomes an Event Hub deep link', () => {
  assert.equal(
    calendarEventHubUrl({
      occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
      notion_url: '',
    }),
    'https://music.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea'
  );
});

test('only event identities produce Event Hub links', () => {
  assert.equal(calendarEventHubUrl({ occurrence_key: 'hotel:booking-id' }), '');
  assert.equal(calendarEventHubUrl({ notion_url: 'https://www.notion.so/' }), '');
  assert.equal(
    calendarEventHubUrl({ notion_url: 'https://example.com/2b639e4a65a9800aaa682f4e2eed5215' }),
    ''
  );
});

test('frozen legacy main-event descriptions are upgraded without changing other details', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    title: 'Santa Barbara Wedding',
    description: 'Assignments: Drums\n\nNotion Link: https://www.notion.so/2b639e4a65a9800aaa682f4e2eed5215\n\nDress Code:\nBlack suit',
  });
  assert.equal(
    event.description,
    'Assignments: Drums\n\nEvent Link: https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215\n\nDress Code:\nBlack suit'
  );
});

test('timeline links are removed from calendar descriptions', () => {
  assert.equal(
    calendarDescriptionWithoutTimelineLink(
      'Assignments: Drums\n\nTimeline Link: https://music.downbeat.agency/timeline/e/event-id\n\nDress Code:\nBlack suit'
    ),
    'Assignments: Drums\n\nDress Code:\nBlack suit'
  );
  assert.equal(
    calendarDescriptionWithoutTimelineLink(
      'Timeline: https://music.downbeat.agency/timeline/event-id\n\nEvent Link: https://music.downbeat.agency/events/event-id'
    ),
    'Event Link: https://music.downbeat.agency/events/event-id'
  );
});

test('frozen main events lose timeline links while retaining Event Hub links', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    description: 'Timeline Link: https://music.downbeat.agency/timeline/e/2b639e4a65a9800aaa682f4e2eed5215\n\nNotion Link: https://www.notion.so/2b639e4a65a9800aaa682f4e2eed5215',
  });
  assert.equal(
    event.description,
    'Event Link: https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215'
  );
});

test('main-event descriptions use Event Link instead of Notion Link', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  assert.match(source, /calendarEventHubUrl\(event\)/u);
  assert.match(source, /allCalendarEvents\.map\(calendarEventWithEventHubLink\)/u);
  assert.match(source, /`Event Link: \$\{eventHubUrl\}\\n\\n`/u);
  assert.match(source, /description \+= `\\nEvent Link: \$\{eventHubUrl\}\\n`/u);
  assert.match(source, /url: eventHubUrl \|\| ''/u);
  assert.doesNotMatch(source, /`Notion Link: \$\{event\.notion_url\}\\n\\n`/u);
  assert.doesNotMatch(source, /`\\nTimeline Link: \$\{timelineLink\}\\n`/u);
});
