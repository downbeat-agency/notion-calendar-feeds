import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assignmentLines,
  assignmentsBlock,
  payDisplay,
  totalPayLine,
} from './calendar-assignment-presentation.js';

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
