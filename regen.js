#!/usr/bin/env node
// Regenerate calendar for one person or Calendar Data page.
// Usage:
//   node regen.js <personId>
//   node regen.js --calendar-data <calendarDataPageIdOrUrl>
// BASE_URL env overrides (e.g. http://localhost:3000).

const BASE_URL = process.env.BASE_URL || 'https://notion-calendar-feeds-production.up.railway.app';
const args = process.argv.slice(2);
const target = args.find(a => !a.startsWith('--')) || '';
const useCalendarDataRoute = args.includes('--calendar-data');

if (!target) {
  console.error('Usage: node regen.js <personId>');
  console.error('   or: node regen.js --calendar-data <calendarDataPageIdOrUrl>');
  process.exit(1);
}

async function regen() {
  const baseUrl = BASE_URL.replace(/\/$/, '');
  const url = useCalendarDataRoute
    ? `${baseUrl}/calendar-data/regenerate?id=${encodeURIComponent(target)}`
    : `${baseUrl}/regenerate/${target}`;
  console.log(`Regenerating calendar for ${target}...`);
  try {
    const res = await fetch(url, { method: 'GET' });
    const data = await res.json().catch(() => ({}));
    if (data.success && data.personName) {
      console.log(`OK: ${data.personName} – ${data.eventCount} events`);
    } else {
      console.error('Failed:', data.message || data.error || res.status);
      process.exit(1);
    }
  } catch (e) {
    console.error('Error:', e.message);
    process.exit(1);
  }
}

regen();
