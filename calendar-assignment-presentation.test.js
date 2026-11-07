import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assignmentLines,
  assignmentsBlock,
  payDisplay,
  personalMainEventDescription,
  totalPayLine,
} from './calendar-assignment-presentation.js';
import {
  calendarEventWithEventHubLink,
  calendarPersonnelEventWithAppLink,
} from './calendar-event-links.js';

test('one line per segment, add-ons merged at the end', () => {
  assert.equal(
    assignmentsBlock('🟢  Event - @Keys\nAfter Party - @DJ\nAdditional - MD\nAdditional - DJ'),
    'Assignments:\nEvent – Keys\nAfter Party – DJ\nAdd-ons – MD, DJ\n'
  );
});

test('empty segments are skipped and add-ons keep their order', () => {
  assert.deepEqual(
    assignmentLines('🟢  Event - @Ceremony Sound\nCeremony - \nAdditional - Photo/Video\nAdditional - Driver\nAdditional - Per Diem'),
    ['Event – Ceremony Sound', 'Add-ons – Photo/Video, Driver, Per Diem']
  );
});

test('a plain role still gets its own line under the label', () => {
  assert.equal(assignmentsBlock('Drums'), 'Assignments:\nDrums\n');
});

test('duplicates are dropped and other status dots are removed', () => {
  assert.deepEqual(
    assignmentLines('🟡 Event - @Bass\nEvent - Bass\nAdditional - MD\nAdditional - md'),
    ['Event – Bass', 'Add-ons – MD']
  );
});

test('nothing to show gives an empty block', () => {
  assert.equal(assignmentsBlock(''), '');
  assert.equal(assignmentsBlock('🟢 \nCeremony - '), '');
  assert.equal(assignmentsBlock(null), '');
});

test('pay is shown with a thousands separator on one line', () => {
  assert.equal(payDisplay('2025'), '$2,025');
  assert.equal(payDisplay('$2025'), '$2,025');
  assert.equal(payDisplay('1,250.5'), '$1,250.50');
  assert.equal(payDisplay(450), '$450');
  assert.equal(totalPayLine('2025'), 'Total Pay: $2,025 (includes add-ons)\n');
});

const GENERAL_INFO = [
  'EVENT DETAILS\n\nLoad In / Parking\nUse the loading dock.\n\nDress Code\nBlack suit\n\nPower\nAV provides drops\n\nGreen Room\nVendor room',
  'CONTRACTED\n\n• Event · 5:30 PM–9:30 PM\n  Band + Horns',
].join('\n\n');

function gigDescription() {
  return personalMainEventDescription({
    calltimeInfo: 'Call Time: 1:30 PM\n\n',
    payrollInfo: `${assignmentsBlock('🟢 Event - @Keys\nAdditional - MD')}${totalPayLine('2025')}\n`,
    gearChecklistInfo: 'GEAR CHECKLIST\nIn-ears\n\n',
    generalInfo: GENERAL_INFO,
    eventPersonnelInfo: 'BAND\nAndrew | Keys | MD\nEli | Drums | Call: 1:30 PM\n\n',
    eventUrlInfo: 'Event Link: https://music.downbeat.agency/events/629d90a3-30f3-4a11-8b78-6c710a937804\n\n',
  });
}

test('personal gig notes put day-of info first and keep every section', () => {
  const { description } = calendarPersonnelEventWithAppLink({
    type: 'main_event',
    occurrence_key: 'event:629d90a3-30f3-4a11-8b78-6c710a937804',
    description: gigDescription(),
  });
  const order = [
    'Call Time: 1:30 PM',
    'Assignments:\nEvent – Keys\nAdd-ons – MD',
    'Total Pay: $2,025 (includes add-ons)',
    'GEAR CHECKLIST\nIn-ears',
    'LOAD IN / PARKING\nUse the loading dock.',
    'DRESS CODE\nBlack suit',
    'POWER\nAV provides drops',
    'GREEN ROOM\nVendor room',
    'CONTRACTED',
    'BAND\nAndrew | Keys | MD\nEli | Drums | Call: 1:30 PM',
    'LINKS',
  ];
  let cursor = -1;
  for (const part of order) {
    const index = description.indexOf(part);
    assert.ok(index > cursor, `expected "${part}" after the previous section in:\n${description}`);
    cursor = index;
  }
  assert.doesNotMatch(description, /EVENT DETAILS|➡️|👥|🔧|PCO/u);
});

test('the shared admin calendar keeps its existing headings', () => {
  const { description } = calendarEventWithEventHubLink({
    type: 'main_event',
    occurrence_key: 'event:629d90a3-30f3-4a11-8b78-6c710a937804',
    description: GENERAL_INFO,
  });
  assert.match(description, /EVENT DETAILS\n\nLoad In \/ Parking\nUse the loading dock\./u);
});
