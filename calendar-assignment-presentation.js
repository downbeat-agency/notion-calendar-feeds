// Formats the payroll role text for a gig's calendar notes.
//
// Source text arrives as one line per seat, e.g.
//   "🟢  Event - @Keys\nAfter Party - @DJ\nAdditional - MD\nAdditional - DJ"
// and is shown as one line per segment, with add-ons merged at the end:
//   Assignments:
//   Event – Keys
//   After Party – DJ
//   Add-ons – MD, DJ

const ADD_ON_SEGMENTS = new Set(['additional', 'add-on', 'add-ons', 'addon', 'addons']);

function cleanPart(value) {
  return String(value ?? '')
    .replace(/@/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Drops leading status markers (🟢, 🟡, bullets) that come from the old formula.
function stripLeadingMarkers(line) {
  return String(line ?? '').replace(/^[^\p{L}\p{N}]+/u, '').trim();
}

export function assignmentLines(value) {
  if (value === null || value === undefined) return [];
  const seats = [];
  const addOns = [];
  const seen = new Set();
  const seenAddOns = new Set();

  for (const rawLine of String(value).split(/\r?\n/)) {
    const line = stripLeadingMarkers(rawLine);
    if (!line) continue;

    const split = line.match(/^(.*?)\s+[-–—]\s*(.*)$/);
    if (!split) {
      const text = cleanPart(line);
      const key = text.toLowerCase();
      if (text && !seen.has(key)) {
        seen.add(key);
        seats.push(text);
      }
      continue;
    }

    const segment = cleanPart(split[1]);
    const role = cleanPart(split[2]);
    if (!role) continue;

    if (ADD_ON_SEGMENTS.has(segment.toLowerCase())) {
      const key = role.toLowerCase();
      if (!seenAddOns.has(key)) {
        seenAddOns.add(key);
        addOns.push(role);
      }
      continue;
    }

    const text = segment ? `${segment} – ${role}` : role;
    const key = text.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      seats.push(text);
    }
  }

  if (addOns.length) seats.push(`Add-ons – ${addOns.join(', ')}`);
  return seats;
}

// Returns the "Assignments:" block (ending in a newline), or '' when there is nothing to show.
export function assignmentsBlock(value) {
  const lines = assignmentLines(value);
  if (!lines.length) return '';
  return `Assignments:\n${lines.join('\n')}\n`;
}

// "$2025" / "2025" / 2025 -> "$2,025"; anything unparseable is shown as given with a "$".
export function payDisplay(payTotalStr) {
  const raw = String(payTotalStr ?? '').trim();
  const numeric = Number(raw.replace(/[$,]/g, ''));
  if (raw && Number.isFinite(numeric)) {
    const hasCents = Math.round(numeric * 100) % 100 !== 0;
    return `$${numeric.toLocaleString('en-US', {
      minimumFractionDigits: hasCents ? 2 : 0,
      maximumFractionDigits: 2,
    })}`;
  }
  return raw.startsWith('$') ? raw : `$${raw}`;
}

export function totalPayLine(payTotalStr) {
  return `Total Pay: ${payDisplay(payTotalStr)} (includes add-ons)\n`;
}
