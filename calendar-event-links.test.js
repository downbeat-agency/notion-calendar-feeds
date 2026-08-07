import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  calendarAppUrl,
  calendarContractUpdatedLabel,
  calendarDescriptionWithoutTimelineLink,
  calendarEventDetailsUpdatedLabel,
  calendarEventHubUrl,
  calendarEventWithEventHubLink,
  calendarMainEventTitle,
  calendarRehearsalTitle,
  calendarTeamEventUrl,
  calendarTimelineUpdatedLabel,
  calendarTravelLinkLabel,
} from './calendar-event-links.js';

test('Event Hub travel links receive booking-specific labels', () => {
  assert.equal(
    calendarTravelLinkLabel(
      'https://music.downbeat.agency/events/event-1?section=travel&travel=hotels',
      'hotels'
    ),
    'Hotel Details'
  );
  assert.equal(
    calendarTravelLinkLabel(
      'https://music.downbeat.agency/events/event-1?section=travel&travel=flights',
      'flights'
    ),
    'Flight Details'
  );
  assert.equal(
    calendarTravelLinkLabel(
      'https://music.downbeat.agency/events/event-1?section=travel&travel=ground',
      'ground'
    ),
    'Ground Details'
  );
});

test('legacy Notion travel URLs keep their honest label', () => {
  assert.equal(
    calendarTravelLinkLabel('https://www.notion.so/hotel-page', 'hotels'),
    'Notion Link'
  );
});

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

test('Postgres occurrence identity becomes the canonical App link', () => {
  assert.equal(
    calendarAppUrl({
      occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    }),
    'https://app.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea'
  );
  assert.equal(
    calendarAppUrl({
      occurrenceKey: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    }),
    'https://app.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea'
  );
});

test('main event titles include the band consistently', () => {
  assert.equal(
    calendarMainEventTitle({ event_name: 'Los Olivos Wedding', band: 'AMFM' }),
    '🎸 Los Olivos Wedding (AMFM)'
  );
  assert.equal(
    calendarMainEventTitle({ event_name: 'San Diego Wedding' }),
    '🎸 San Diego Wedding'
  );
});

test('rehearsal titles show the band without repeating the linked event', () => {
  assert.equal(
    calendarRehearsalTitle({
      event_name: 'Orange County Wedding',
      band: 'The A-List',
    }),
    '🎤 Rehearsal (The A-List)'
  );
  assert.equal(calendarRehearsalTitle({}), '🎤 Rehearsal');
});

test('office calendar entries prefer the authenticated Postgres schedule link', () => {
  assert.equal(
    calendarTeamEventUrl({
      schedule_url: 'https://music.downbeat.agency/time?shift=shift-1&date=2026-08-07',
      notion_link: 'https://www.notion.so/legacy-office-row',
    }),
    'https://music.downbeat.agency/time?shift=shift-1&date=2026-08-07'
  );
  assert.equal(calendarTeamEventUrl({ schedule_url: 'javascript:alert(1)' }), '');
});

test('calendar update labels format exact Postgres instants and date-only history honestly', () => {
  assert.equal(
    calendarTimelineUpdatedLabel({
      timeline_updated_at: '2026-08-06T23:28:00.000Z',
      timeline_updated_precision: 'timestamp',
    }),
    'August 6, 2026 at 4:28 PM PDT'
  );
  assert.equal(
    calendarEventDetailsUpdatedLabel({
      event_details_updated_at: '2026-08-06T23:28:00.000Z',
      event_details_updated_precision: 'timestamp',
    }),
    'August 6, 2026 at 4:28 PM PDT'
  );
  assert.equal(
    calendarEventDetailsUpdatedLabel({
      event_details_updated_at: '2026-04-07',
      event_details_updated_precision: 'date',
    }),
    'April 7, 2026'
  );
  assert.equal(
    calendarContractUpdatedLabel({
      contract_updated_at: '2026-03-01',
      contract_updated_precision: 'date',
    }),
    'March 1, 2026'
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
    [
      'Assignments: Drums',
      '',
      'Dress Code',
      'Black suit',
      '',
      'LINKS',
      '',
      'Event Link: https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
      'App Link: https://app.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
    ].join('\n')
  );
  assert.equal(
    event.url,
    'https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215'
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
    [
      'LINKS',
      '',
      'Event Link: https://music.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
      'App Link: https://app.downbeat.agency/events/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
    ].join('\n')
  );
});

test('main-event descriptions repair escaped copy and remove empty or obsolete metadata', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    description: [
      'Parking and Load In:',
      '\\n',
      'Dress Code:',
      'Black formal\\n',
      'Green Room:',
      '\\n',
      'Event Notes:',
      '• Bridgerton vibes for cocktail hour\\n',
      'Day of Contact for Band: Brandon Shaw - (818) 913-2487\\n',
      'Contracted:',
      'Ceremony: Ceremony Sound + Violin + Keys 3:30pm-4:30pm (1hrs)\\n',
      'PCO Link: https://services.planningcenteronline.com/plans/84911226\\n',
      'App Link: https://app.downbeat.agency/event/02e26e6b-efb4-419c-9486-6cd8265c40ea\\n',
      'Notes Updated: April 7, 2026 5:57 PM',
    ].join(''),
  });

  assert.doesNotMatch(event.description, /\\n/u);
  assert.doesNotMatch(event.description, /Parking and Load In|Green Room|Notes Updated/u);
  assert.match(
    event.description,
    /App Link: https:\/\/app\.downbeat\.agency\/events\/02e26e6b-efb4-419c-9486-6cd8265c40ea/u
  );
  assert.match(event.description, /Dress Code\nBlack formal/u);
  assert.match(event.description, /Event Notes\n• Bridgerton vibes for cocktail hour/u);
  assert.match(event.description, /Day-of Contact\nBrandon Shaw - \(818\) 913-2487/u);
  assert.match(
    event.description,
    /CONTRACTED\n\n• Ceremony · 3:30 PM–4:30 PM \(1hrs\)\n  Ceremony Sound \+ Violin \+ Keys/u
  );
  assert.match(
    event.description,
    /LINKS\n\nEvent Link: https:\/\/music\.downbeat\.agency\/events\/02e26e6b-efb4-419c-9486-6cd8265c40ea\nApp Link: https:\/\/app\.downbeat\.agency\/events\/02e26e6b-efb4-419c-9486-6cd8265c40ea\nPCO Plan: https:\/\/services\.planningcenteronline\.com\/plans\/84911226\n\nEvent Details Updated: April 7, 2026 5:57 PM$/u
  );
  assert.equal(
    event.url,
    'https://music.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea'
  );
});

test('Postgres metadata overrides legacy update copy without leaking projection fields', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    appUrl: 'https://app.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea',
    timelineUpdatedAt: '2026-08-06T23:28:00.000Z',
    timelineUpdatedPrecision: 'timestamp',
    eventDetailsUpdatedAt: '2026-08-05T18:00:00.000Z',
    eventDetailsUpdatedPrecision: 'timestamp',
    contractUpdatedAt: '2026-08-04T20:00:00.000Z',
    contractUpdatedPrecision: 'timestamp',
    description: [
      'Timeline Updated: July 1, 2026 1:00 PM',
      'Notes Updated: April 7, 2026 5:57 PM',
      'Contract Updated: March 1, 2026',
    ].join('\n'),
  });
  assert.match(event.description, new RegExp(
    [
      'Timeline Updated: August 6, 2026 at 4:28 PM PDT',
      'Event Details Updated: August 5, 2026 at 11:00 AM PDT',
      'Contract Updated: August 4, 2026 at 1:00 PM PDT$',
    ].join('\\n'),
    'u'
  ));
  assert.doesNotMatch(
    event.description,
    /July 1|Notes Updated|April 7|March 1/u
  );
  assert.equal(Object.hasOwn(event, 'appUrl'), false);
  assert.equal(Object.hasOwn(event, 'timelineUpdatedAt'), false);
  assert.equal(Object.hasOwn(event, 'timelineUpdatedPrecision'), false);
  assert.equal(Object.hasOwn(event, 'eventDetailsUpdatedAt'), false);
  assert.equal(Object.hasOwn(event, 'eventDetailsUpdatedPrecision'), false);
  assert.equal(Object.hasOwn(event, 'contractUpdatedAt'), false);
  assert.equal(Object.hasOwn(event, 'contractUpdatedPrecision'), false);
});

test('raw Postgres update metadata is also stripped from decorated events', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    app_url: 'https://app.downbeat.agency/events/02e26e6b-efb4-419c-9486-6cd8265c40ea',
    timeline_updated_at: '2026-08-06T23:28:00.000Z',
    timeline_updated_precision: 'timestamp',
    event_details_updated_at: '2026-08-05T18:00:00.000Z',
    event_details_updated_precision: 'timestamp',
    contract_updated_at: '2026-08-04T20:00:00.000Z',
    contract_updated_precision: 'timestamp',
    description: '',
  });
  assert.equal(Object.hasOwn(event, 'app_url'), false);
  assert.equal(Object.hasOwn(event, 'timeline_updated_at'), false);
  assert.equal(Object.hasOwn(event, 'timeline_updated_precision'), false);
  assert.equal(Object.hasOwn(event, 'event_details_updated_at'), false);
  assert.equal(Object.hasOwn(event, 'event_details_updated_precision'), false);
  assert.equal(Object.hasOwn(event, 'contract_updated_at'), false);
  assert.equal(Object.hasOwn(event, 'contract_updated_precision'), false);
});

test('main-event descriptions use Event Link instead of Notion Link', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  assert.match(source, /calendarEventHubUrl\(event\)/u);
  assert.match(source, /const title = calendarMainEventTitle\(event\);/u);
  assert.match(source, /allCalendarEvents\.map\(calendarEventWithEventHubLink\)/u);
  assert.match(
    source,
    /function processAdminEvents[\s\S]*?return allCalendarEvents\.map\(calendarEventWithEventHubLink\);/u
  );
  assert.match(source, /`Event Link: \$\{eventHubUrl\}\\n\\n`/u);
  assert.match(source, /description \+= `\\nEvent Link: \$\{eventHubUrl\}\\n`/u);
  assert.match(source, /url: eventHubUrl \|\| ''/u);
  assert.match(source, /appUrl: source\?\.app_url \|\| undefined/u);
  assert.match(source, /timelineUpdatedAt: source\?\.timeline_updated_at \|\| undefined/u);
  assert.match(source, /eventDetailsUpdatedAt: source\?\.event_details_updated_at \|\| undefined/u);
  assert.match(source, /contractUpdatedAt: source\?\.contract_updated_at \|\| undefined/u);
  assert.match(source, /url: rehearsal\.rehearsal_link \|\| ''/u);
  assert.doesNotMatch(source, /`Notion Link: \$\{event\.notion_url\}\\n\\n`/u);
  assert.doesNotMatch(source, /`\\nTimeline Link: \$\{timelineLink\}\\n`/u);
});
