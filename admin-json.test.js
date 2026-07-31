import assert from 'node:assert/strict';
import test from 'node:test';
import {
  escapeJsonStringControlCharacters,
  parseNotionFormulaJsonArray
} from './admin-json.js';

test('escapes literal control characters inside JSON strings', () => {
  const raw = '[{"general_info":"Line one\tLine two\nLine three\u0008"}]';

  assert.throws(() => JSON.parse(raw), /control character/i);
  assert.deepEqual(parseNotionFormulaJsonArray(raw), [{
    general_info: 'Line one\tLine two\nLine three\b'
  }]);
});

test('preserves valid escaped JSON and structural whitespace', () => {
  const raw = '[\n  {"general_info":"Already escaped\\tvalue","enabled":true}\n]';

  assert.equal(escapeJsonStringControlCharacters(raw), raw);
  assert.deepEqual(parseNotionFormulaJsonArray(raw), [{
    general_info: 'Already escaped\tvalue',
    enabled: true
  }]);
});

test('returns an empty array for empty input or non-array JSON', () => {
  assert.deepEqual(parseNotionFormulaJsonArray(''), []);
  assert.deepEqual(parseNotionFormulaJsonArray('{"event_name":"Example"}'), []);
});
