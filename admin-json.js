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

export function parseNotionFormulaJsonArray(raw) {
  const parsed = JSON.parse(escapeJsonStringControlCharacters(raw || '[]'));
  return Array.isArray(parsed) ? parsed : [];
}
