// Test script for the new ISO date format parser
import ical from 'ical-generator';

// New simple parser for ISO date format
function parseISODateTime(dateTimeStr) {
  if (!dateTimeStr) return null;
  
  try {
    // Split on the forward slash to get start and end times
    const [startStr, endStr] = dateTimeStr.split('/');
    
    if (!startStr || !endStr) {
      console.warn('Invalid ISO date format:', dateTimeStr);
      return null;
    }
    
    // Parse the ISO strings directly - JavaScript handles timezone conversion automatically
    const start = new Date(startStr.trim());
    const end = new Date(endStr.trim());
    
    // Validate the dates
    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
      console.warn('Invalid date parsing:', { startStr, endStr });
      return null;
    }
    
    return { start, end };
  } catch (e) {
    console.warn('Failed to parse ISO date:', dateTimeStr, e);
    return null;
  }
}

console.log('=== TESTING NEW ISO DATE PARSER ===\n');

// Test with your new data format
const testEvents = [
  {
    name: "Los Angeles Event",
    date: "2025-09-06T19:00:00-07:00/2025-09-06T23:00:00-07:00"
  },
  {
    name: "Pasadena Event", 
    date: "2025-10-04T17:30:00-07:00/2025-10-04T22:00:00-07:00"
  },
  {
    name: "Pacific Palisades Wedding",
    date: "2025-11-07T15:00:00-08:00/2025-11-07T22:00:00-08:00"
  },
  {
    name: "Demarest Wedding (Multi-day)",
    date: "2025-10-11T17:00:00-07:00/2025-10-12T00:30:00-07:00"
  }
];

testEvents.forEach(event => {
  console.log(`\n--- ${event.name} ---`);
  console.log('Input:', event.date);
  
  const parsed = parseISODateTime(event.date);
  if (parsed) {
    console.log('Start:', parsed.start.toString());
    console.log('End:', parsed.end.toString());
    console.log('Start ISO:', parsed.start.toISOString());
    console.log('End ISO:', parsed.end.toISOString());
    
    // Test ICS generation
    const calendar = ical({ name: 'Test Calendar' });
    calendar.createEvent({
      start: parsed.start,
      end: parsed.end,
      summary: event.name,
      description: 'Test event with new ISO parser',
      floating: true
    });
    
    const ics = calendar.toString();
    const dtstartMatch = ics.match(/DTSTART:(\d{8}T\d{6})/);
    const dtendMatch = ics.match(/DTEND:(\d{8}T\d{6})/);
    
    console.log('ICS DTSTART:', dtstartMatch ? dtstartMatch[1] : 'NOT FOUND');
    console.log('ICS DTEND:', dtendMatch ? dtendMatch[1] : 'NOT FOUND');
  } else {
    console.log('❌ FAILED TO PARSE');
  }
});

console.log('\n=== COMPARISON WITH OLD FORMAT ===\n');

// Compare with old format
const oldFormat = "@October 4, 2025 3:00 PM → 10:00 PM";
const newFormat = "2025-10-04T17:30:00-07:00/2025-10-04T22:00:00-07:00";

console.log('Old format:', oldFormat);
console.log('New format:', newFormat);

const oldParsed = parseISODateTime(oldFormat); // This should fail
const newParsed = parseISODateTime(newFormat);

console.log('Old format result:', oldParsed ? 'SUCCESS' : 'FAILED (expected)');
console.log('New format result:', newParsed ? 'SUCCESS' : 'FAILED');

if (newParsed) {
  console.log('New format start:', newParsed.start.toString());
  console.log('New format end:', newParsed.end.toString());
}
