/**
 * Notion formulas occasionally return literal control characters inside JSON
 * string values. JSON.parse rejects those characters unless they are escaped.
 */
export function escapeJsonStringControlCharacters(raw) {
  const source = typeof raw === 'string' ? raw : '';
  let result = '';
  let insideString = false;
  let escaped = false;

  for (const character of source) {
    if (!insideString) {
      result += character;
      if (character === '"') {
        insideString = true;
      }
      continue;
    }

    if (escaped) {
      result += character;
      escaped = false;
      continue;
    }

    if (character === '\\') {
      result += character;
      escaped = true;
      continue;
    }

    if (character === '"') {
      result += character;
      insideString = false;
      continue;
    }

    const codePoint = character.codePointAt(0);
    if (codePoint <= 0x1f) {
      result += `\\u${codePoint.toString(16).padStart(4, '0')}`;
      continue;
    }

    result += character;
  }

  return result;
}

export function fillMissingJsonValues(raw) {
  const source = typeof raw === 'string' ? raw : '';
  let result = '';
  let insideString = false;
  let escaped = false;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];

    if (insideString) {
      result += character;
      if (escaped) {
        escaped = false;
      } else if (character === '\\') {
        escaped = true;
      } else if (character === '"') {
        insideString = false;
      }
      continue;
    }

    if (character === '"') {
      insideString = true;
      result += character;
      continue;
    }

    result += character;
    if (character !== ':') {
      continue;
    }

    let valueIndex = index + 1;
    while (
      valueIndex < source.length &&
      [' ', '\t', '\n', '\r'].includes(source[valueIndex])
    ) {
      result += source[valueIndex];
      valueIndex += 1;
    }

    if (
      valueIndex < source.length &&
      [',', '}', ']'].includes(source[valueIndex])
    ) {
      result += 'null';
    }
    index = valueIndex - 1;
  }

  return result;
}

export function parseNotionFormulaJsonArray(raw) {
  const escaped = escapeJsonStringControlCharacters(raw || '[]');
  const parsed = JSON.parse(fillMissingJsonValues(escaped));
  return Array.isArray(parsed) ? parsed : [];
}
