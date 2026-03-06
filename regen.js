#!/usr/bin/env node
// Regenerate calendar for one person. Usage: node regen.js <personId>
// personId can be 32-char hex or UUID. BASE_URL env overrides (e.g. http://localhost:3000).

const BASE_URL = process.env.BASE_URL || 'https://notion-calendar-feeds-production.up.railway.app';
const args = process.argv.slice(2);
const personId = args.find(a => !a.startsWith('--')) || '';

if (!personId) {
  console.error('Usage: node regen.js <personId>');
  process.exit(1);
}

async function regen() {
  const baseUrl = BASE_URL.replace(/\/$/, '');
  const url = `${baseUrl}/regenerate/${personId}`;
  console.log(`Regenerating calendar for ${personId}...`);
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
