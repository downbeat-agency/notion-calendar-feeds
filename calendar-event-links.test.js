import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ical from 'ical-generator';
import {
  calendarAppUrl,
  calendarContractUpdatedLabel,
  calendarDescriptionWithoutPcoLink,
  calendarDescriptionWithoutTimelineLink,
  calendarEventDetailsUpdatedLabel,
  calendarEventHubUrl,
  calendarEventWithEventHubLink,
  calendarMainEventTitle,
  calendarPersonnelEventWithAppLink,
  calendarPersonnelTravelDescriptionUrl,
  calendarPersonnelTravelDescriptionLink,
  calendarRehearsalAppUrl,
  calendarRehearsalTitle,
  calendarAdminRehearsalTitle,
  calendarTeamEventUrl,
  calendarTimelineUpdatedLabel,
  calendarTravelLinkLabel,
} from './calendar-event-links.js';

const POSTGRES_EVENT_ID = 'eff09abf-522b-4fcd-b3f6-446a3f6eccf6';
const EVENT_NOTION_PAGE_ID = '23939e4a-65a9-8032-be86-df28748fcd61';
const EVENT_APP_URL = `https://app.downbeat.agency/event/${EVENT_NOTION_PAGE_ID}`;

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

test('personnel travel notes prefer the web destination and never repeat the app URL', () => {
  const web = `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}?section=travel&travel=flights`;
  const app = `https://app.downbeat.agency/event/${EVENT_NOTION_PAGE_ID}/itinerary/flight-leg-77777777-7777-4777-8777-777777777777`;
  assert.equal(calendarPersonnelTravelDescriptionUrl(web, app), web);
  assert.equal(calendarPersonnelTravelDescriptionUrl('', app), '');
  assert.equal(calendarPersonnelTravelDescriptionUrl('', web), web);
  assert.equal(calendarPersonnelTravelDescriptionUrl('', 'https://www.notion.so/legacy'),
    'https://www.notion.so/legacy');
  assert.equal(calendarPersonnelTravelDescriptionUrl('https://example.test/unrelated', app), '');
  for (const [type, label] of [['flights', 'Flight Details'], ['hotels', 'Hotel Details'], ['ground', 'Ground Details']]) {
    const typedWeb = `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}?section=travel&travel=${type}`;
    assert.equal(calendarPersonnelTravelDescriptionLink(typedWeb, app, type), `${label}: ${typedWeb}`);
    assert.equal(calendarPersonnelTravelDescriptionLink('', app, type), '');
  }
});

test('personnel flight, hotel, ground, and rehearsal ICS keep app URL and web notes separate', () => {
  const types = [
    ['flight_departure', 'flights', 'Flight Details', 'flight-leg-77777777-7777-4777-8777-777777777777'],
    ['hotel', 'hotels', 'Hotel Details', 'hotel-88888888-8888-4888-8888-888888888888-check-in'],
    ['ground_transport_meeting', 'ground', 'Ground Details', 'ground-stop-aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa-meetup'],
  ];
  const cases = types.map(([type, tab, label, item]) => {
    const app = `${EVENT_APP_URL}/itinerary/${item}`;
    const web = `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}?section=travel&travel=${tab}`;
    return calendarPersonnelEventWithAppLink({
      type, url: app,
      description: `Keep gate A, then B; call dispatch\\now.\n\n${calendarPersonnelTravelDescriptionLink(web, app, tab)}`,
      expectedApp: app, expectedWeb: `${label}: ${web}`,
    });
  });
  const rehearsalApp = 'https://app.downbeat.agency/rehearsal/33333333-3333-4333-8333-333333333333';
  const rehearsalWeb = `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}?section=rehearsals&rehearsalId=33333333-3333-4333-8333-333333333333`;
  cases.push(calendarPersonnelEventWithAppLink({ type: 'rehearsal',
    app_link: rehearsalApp, rehearsal_link: rehearsalWeb,
    description: 'Full run, then notes; keep the accents José.',
    expectedApp: rehearsalApp, expectedWeb: `Rehearsal Link: ${rehearsalWeb}`,
  }));
  for (const [index, item] of cases.entries()) {
    const refreshed = calendarPersonnelEventWithAppLink(item);
    assert.equal(refreshed.url, item.url);
    assert.equal(refreshed.description, item.description);
    const calendar = ical({ name: 'Downbeat' });
    calendar.createEvent({ id: `stable-${index}@calendar.downbeat.agency`,
      start: new Date('2026-10-03T16:00:00Z'), end: new Date('2026-10-03T17:00:00Z'),
      summary: 'Travel', description: item.description, url: item.url });
    const output = calendar.toString();
    const unfolded = output.replace(/\r\n /gu, '');
    const description = unfolded.split('\r\n').find((line) => line.startsWith('DESCRIPTION:')) || '';
    assert.equal(item.url, item.expectedApp);
    assert.ok(unfolded.includes(`URL;VALUE=URI:${item.expectedApp}`));
    assert.ok(description.includes(item.expectedWeb));
    assert.equal(description.split(item.expectedWeb).length - 1, 1);
    assert.doesNotMatch(description, /app\.downbeat\.agency/u);
    assert.match(unfolded, new RegExp(`UID:stable-${index}@calendar\\.downbeat\\.agency`, 'u'));
    if (index < 3) assert.match(description, /gate A\\, then B\\; call dispatch/u);
    else assert.match(description, /notes\\; keep the accents José/u);
    assert.ok(output.split('\r\n').every((line) => Buffer.byteLength(line, 'utf8') <= 75));
  }
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

test('App links require the distinct Notion identity and singular app route', () => {
  assert.equal(
    calendarAppUrl({
      occurrence_key: `event:${POSTGRES_EVENT_ID}`,
    }),
    ''
  );
  assert.equal(
    calendarAppUrl({
      notion_url: `https://www.notion.so/${EVENT_NOTION_PAGE_ID.replaceAll('-', '')}`,
    }),
    EVENT_APP_URL
  );
  assert.equal(
    calendarAppUrl({ app_url: EVENT_APP_URL }),
    EVENT_APP_URL
  );
  assert.equal(
    calendarAppUrl({
      app_url: `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}`,
    }),
    ''
  );
});

test('rehearsal App links accept source aliases and reject non-web URLs', () => {
  const appLink = 'https://app.downbeat.agency/events/event-id?section=rehearsals';
  assert.equal(calendarRehearsalAppUrl({ app_link: appLink }), appLink);
  assert.equal(calendarRehearsalAppUrl({ appUrl: appLink }), appLink);
  assert.equal(calendarRehearsalAppUrl({ app_link: 'javascript:alert(1)' }), '');
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

test('rehearsal links render after pay and personnel with readable spacing', () => {
  const event = calendarEventWithEventHubLink({
    type: 'rehearsal',
    description: [
      'Rehearsal for Gold Standard',
      '',
      'LINKS',
      '',
      'Rehearsal Link: https://music.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
      'App Link: https://app.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
      '',
      'Rehearsal Pay - $100',
      '',
      'Band Personnel:',
      'Bass: Brandon',
      'Drums: Diego',
    ].join('\n'),
    rehearsalLink: 'https://music.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
    appUrl: 'https://app.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
    url: 'https://services.planningcenteronline.com/plans/123',
  });

  assert.equal(
    event.description,
    [
      'Rehearsal for Gold Standard',
      '',
      'Rehearsal Pay - $100',
      '',
      'Band Personnel:',
      'Bass: Brandon',
      'Drums: Diego',
      '',
      'LINKS',
      '',
      'Rehearsal Link: https://music.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
      '',
      'App Link: https://app.downbeat.agency/events/event-1?section=rehearsals&rehearsalId=rehearsal-1',
    ].join('\n')
  );
  assert.equal(event.url, 'https://services.planningcenteronline.com/plans/123');
  assert.equal(Object.hasOwn(event, 'rehearsalLink'), false);
  assert.equal(Object.hasOwn(event, 'appUrl'), false);
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
      'App Link: https://app.downbeat.agency/event/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
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

test('PCO links are removed from personnel calendar descriptions', () => {
  assert.equal(
    calendarDescriptionWithoutPcoLink([
      'Full run',
      '',
      'LINKS',
      '',
      'PCO Link: https://services.planningcenteronline.com/plans/123',
    ].join('\n')),
    'Full run'
  );
  assert.equal(
    calendarDescriptionWithoutPcoLink('Flight details\n\n\nKeep original spacing'),
    'Flight details\n\n\nKeep original spacing'
  );
});

test('personnel main events omit PCO and publish the App link as the calendar URL', () => {
  const event = calendarPersonnelEventWithAppLink({
    type: 'main_event',
    occurrenceKey: `event:${POSTGRES_EVENT_ID}`,
    appUrl: EVENT_APP_URL,
    url: 'https://services.planningcenteronline.com/plans/84911226',
    description: [
      'Dress Code:',
      'Black formal',
      '',
      `Event Link: https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}`,
      'PCO Plan: https://services.planningcenteronline.com/plans/84911226',
    ].join('\n'),
  });

  assert.equal(event.url, EVENT_APP_URL);
  assert.doesNotMatch(event.description, /PCO|planningcenter/iu);
  assert.match(event.description, new RegExp(`Event Link: https://music\\.downbeat\\.agency/events/${POSTGRES_EVENT_ID}`, 'u'));
  assert.match(event.description, new RegExp(`App Link: ${EVENT_APP_URL}`, 'u'));
});

test('personnel main events never turn an Event Link into an App Link', () => {
  const event = calendarPersonnelEventWithAppLink({
    type: 'main_event',
    occurrenceKey: `event:${POSTGRES_EVENT_ID}`,
    url: `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}`,
    description: `Event Link: https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}`,
  });

  assert.equal(event.url, '');
  assert.doesNotMatch(event.description, /App Link:/u);
});

test('personnel rehearsals keep the App URL and show the matching web link once in notes', () => {
  const appLink = 'https://app.downbeat.agency/rehearsal/33333333-3333-4333-8333-333333333333';
  const webLink = `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}?section=rehearsals&rehearsalId=33333333-3333-4333-8333-333333333333`;
  const rehearsal = calendarPersonnelEventWithAppLink({
    type: 'rehearsal',
    appUrl: appLink,
    rehearsalLink: webLink,
    url: 'https://services.planningcenteronline.com/plans/123',
    description: `Full run\n\nLINKS\n\nApp Link: ${appLink}\n\nPCO Link: https://services.planningcenteronline.com/plans/123`,
  });
  assert.equal(rehearsal.url, appLink);
  assert.equal(rehearsal.description, `Full run\n\nLINKS\n\nRehearsal Link: ${webLink}`);
  assert.equal(calendarPersonnelEventWithAppLink(rehearsal).description, rehearsal.description);
  assert.equal(calendarPersonnelEventWithAppLink(rehearsal).url, appLink);
  assert.doesNotMatch(rehearsal.description, /PCO|planningcenter|App Link/iu);
  assert.equal(Object.hasOwn(rehearsal, 'appUrl'), false);

  assert.equal(
    calendarPersonnelEventWithAppLink({
      type: 'rehearsal',
      url: 'https://services.planningcenteronline.com/plans/456',
      description: 'Rehearsal',
    }).url,
    ''
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
      'App Link: https://app.downbeat.agency/event/2b639e4a-65a9-800a-aa68-2f4e2eed5215',
    ].join('\n')
  );
});

test('main-event descriptions repair escaped copy and remove empty or obsolete metadata', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: `event:${POSTGRES_EVENT_ID}`,
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
      `App Link: ${EVENT_APP_URL}\\n`,
      'Notes Updated: April 7, 2026 5:57 PM',
    ].join(''),
  });

  assert.doesNotMatch(event.description, /\\n/u);
  assert.doesNotMatch(event.description, /Parking and Load In|Green Room|Notes Updated/u);
  assert.match(
    event.description,
    new RegExp(`App Link: ${EVENT_APP_URL}`, 'u')
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
    new RegExp(`LINKS\\n\\nEvent Link: https://music\\.downbeat\\.agency/events/${POSTGRES_EVENT_ID}\\nApp Link: ${EVENT_APP_URL}\\nPCO Plan: https://services\\.planningcenteronline\\.com/plans/84911226\\n\\nEvent Details Updated: April 7, 2026 5:57 PM$`, 'u')
  );
  assert.equal(
    event.url,
    `https://music.downbeat.agency/events/${POSTGRES_EVENT_ID}`
  );
});

test('Postgres metadata overrides legacy update copy without leaking projection fields', () => {
  const event = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: 'event:02e26e6b-efb4-419c-9486-6cd8265c40ea',
    appUrl: EVENT_APP_URL,
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
    app_url: EVENT_APP_URL,
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

test('personnel calendars publish App URLs while Admin retains Event Hub URLs', () => {
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  const personalBuilder = source.slice(
    source.indexOf('function buildCalendarEventsFromCalendarData'),
    source.indexOf('async function regenerateCalendarForPersonFromNotion')
  );
  assert.match(source, /calendarEventHubUrl\(event\)/u);
  assert.match(source, /const title = calendarMainEventTitle\(event\);/u);
  assert.match(
    source,
    /const publishedCalendarEvents = allCalendarEvents\.map\(calendarPersonnelEventWithAppLink\);/u
  );
  assert.match(source, /adminCalendarEventsOnly\(allCalendarEvents\)\.map\(calendarEventWithEventHubLink\)/u);
  assert.match(
    source,
    /function processAdminEvents[\s\S]*?return adminCalendarEventsOnly\(allCalendarEvents\)\.map\(calendarEventWithEventHubLink\);/u
  );
  assert.match(source, /`Event Link: \$\{eventHubUrl\}\\n\\n`/u);
  assert.match(source, /description \+= `\\nEvent Link: \$\{eventHubUrl\}\\n`/u);
  assert.match(source, /url: eventHubUrl \|\| ''/u);
  assert.match(source, /appUrl: source\?\.app_url \|\| source\?\.app_link \|\| undefined/u);
  assert.match(source, /timelineUpdatedAt: source\?\.timeline_updated_at \|\| undefined/u);
  assert.match(source, /eventDetailsUpdatedAt: source\?\.event_details_updated_at \|\| undefined/u);
  assert.match(source, /contractUpdatedAt: source\?\.contract_updated_at \|\| undefined/u);
  assert.match(personalBuilder, /url: calendarRehearsalAppUrl\(rehearsal\)/u);
  assert.doesNotMatch(
    personalBuilder,
    /url: rehearsal\.rehearsal_notion_url \|\| rehearsal\.rehearsal_pco/u
  );
  assert.match(
    source,
    /url: rehearsal\.rehearsal_notion_url \|\| rehearsal\.rehearsal_pco \|\| rehearsal\.rehearsal_link \|\| ''/u
  );
  assert.doesNotMatch(source, /`Notion Link: \$\{event\.notion_url\}\\n\\n`/u);
  assert.doesNotMatch(source, /`\\nTimeline Link: \$\{timelineLink\}\\n`/u);
});

test('admin rehearsal title identifies the gig date and city without changing personal titles', () => {
  const event = { band: 'Soultones', event_day: '2026-10-03', event_city: 'Carmel Valley', event_name: 'Carmel Valley Wedding', rehearsal_date: '2026-09-30' };
  assert.equal(calendarAdminRehearsalTitle(event), '🎹 Soultones Rehearsal 10/3/26 Carmel Valley');
  assert.equal(calendarRehearsalTitle(event), '🎤 Rehearsal (Soultones)');
  assert.equal(calendarAdminRehearsalTitle({}), '🎹 Rehearsal');
  assert.equal(calendarAdminRehearsalTitle({ band: 'AMFM', event_name: 'Private Event' }), '🎹 AMFM Rehearsal Private Event');
  const source = readFileSync(new URL('./index.js', import.meta.url), 'utf8');
  assert.match(source.slice(source.indexOf('function processAdminEvents')), /const title = calendarAdminRehearsalTitle\(event\)/u);
});
