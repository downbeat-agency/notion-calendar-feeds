// Test script to debug main event parsing
import ical from 'ical-generator';

// Copy the updated parseUnifiedDateTime function
function parseUnifiedDateTime(dateTimeStr) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  // Clean up the string
  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  
  // Check if it's the new ISO format (contains / and T)
  if (cleanStr.includes('/') && cleanStr.includes('T')) {
    try {
      const [startStr, endStr] = cleanStr.split('/');
      
      if (!startStr || !endStr) {
        console.warn('Invalid ISO date format:', cleanStr);
        return null;
      }
      
      const start = new Date(startStr.trim());
      const end = new Date(endStr.trim());
      
      if (isNaN(start.getTime()) || isNaN(end.getTime())) {
        console.warn('Invalid date parsing:', { startStr, endStr });
        return null;
      }
      
      return { start, end };
    } catch (e) {
      console.warn('Failed to parse ISO date:', cleanStr, e);
      return null;
    }
  }
  
  // Check if it's the unified format with @
  if (cleanStr.startsWith('@')) {
    // ... rest of old logic would go here
    return null;
  }
  
  return null;
}

console.log('=== TESTING MAIN EVENT PARSING ===\n');

// Test with the actual data from the server
const testDate = "2025-09-07T02:00:00+00:00/2025-09-07T06:00:00+00:00";
console.log('Input:', testDate);

const result = parseUnifiedDateTime(testDate);
console.log('Result:', result);

if (result) {
  console.log('Start:', result.start.toString());
  console.log('End:', result.end.toString());
  console.log('Start ISO:', result.start.toISOString());
  console.log('End ISO:', result.end.toISOString());
  
  // Test ICS generation
  const calendar = ical({ name: 'Test Calendar' });
  calendar.createEvent({
    start: result.start,
    end: result.end,
    summary: 'Test Main Event',
    description: 'Test event',
    floating: true
  });
  
  const ics = calendar.toString();
  const dtstartMatch = ics.match(/DTSTART:(\d{8}T\d{6})/);
  const dtendMatch = ics.match(/DTEND:(\d{8}T\d{6})/);
  
  console.log('ICS DTSTART:', dtstartMatch ? dtstartMatch[1] : 'NOT FOUND');
  console.log('ICS DTEND:', dtendMatch ? dtendMatch[1] : 'NOT FOUND');
} else {
  console.log('❌ PARSING FAILED');
}
