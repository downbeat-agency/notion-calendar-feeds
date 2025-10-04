// Test script for simple date parsing without timezone conversion
import ical from 'ical-generator';

// Updated parseUnifiedDateTime function
function parseUnifiedDateTime(dateTimeStr) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  
  if (cleanStr.includes('/') && cleanStr.includes('T')) {
    try {
      const [startStr, endStr] = cleanStr.split('/');
      
      if (!startStr || !endStr) {
        console.warn('Invalid ISO date format:', cleanStr);
        return null;
      }
      
      // Extract the date and time components directly from the ISO string
      const startMatch = startStr.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/);
      const endMatch = endStr.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/);
      
      if (!startMatch || !endMatch) {
        console.warn('Could not parse date components from ISO strings');
        return null;
      }
      
      // Create floating dates using the local time components directly
      const start = new Date(
        parseInt(startMatch[1]), // year
        parseInt(startMatch[2]) - 1, // month (0-indexed)
        parseInt(startMatch[3]), // day
        parseInt(startMatch[4]), // hour
        parseInt(startMatch[5]), // minute
        parseInt(startMatch[6])  // second
      );
      
      const end = new Date(
        parseInt(endMatch[1]), // year
        parseInt(endMatch[2]) - 1, // month (0-indexed)
        parseInt(endMatch[3]), // day
        parseInt(endMatch[4]), // hour
        parseInt(endMatch[5]), // minute
        parseInt(endMatch[6])  // second
      );
      
      return { start, end };
    } catch (e) {
      console.warn('Failed to parse ISO date:', cleanStr, e);
      return null;
    }
  }
  
  return null;
}

console.log('=== TESTING SIMPLE DATE PARSING ===\n');

// Test with the Pasadena Event data
const testDate = "2025-10-04T17:30:00-07:00/2025-10-04T22:00:00-07:00";
console.log('Input (from your new data):', testDate);

const result = parseUnifiedDateTime(testDate);
console.log('Result:', result);

if (result) {
  console.log('\n--- CONVERSION RESULTS ---');
  console.log('Start (Floating):', result.start.toString());
  console.log('End (Floating):', result.end.toString());
  console.log('Start ISO:', result.start.toISOString());
  console.log('End ISO:', result.end.toISOString());
  
  // Test ICS generation
  const calendar = ical({ name: 'Test Calendar' });
  calendar.createEvent({
    start: result.start,
    end: result.end,
    summary: 'Pasadena Event (Gold Standard)',
    description: 'Test event',
    floating: true
  });
  
  const ics = calendar.toString();
  const dtstartMatch = ics.match(/DTSTART:(\d{8}T\d{6})/);
  const dtendMatch = ics.match(/DTEND:(\d{8}T\d{6})/);
  
  console.log('\n--- ICS GENERATION ---');
  console.log('ICS DTSTART:', dtstartMatch ? dtstartMatch[1] : 'NOT FOUND');
  console.log('ICS DTEND:', dtendMatch ? dtendMatch[1] : 'NOT FOUND');
  
  // Expected: Should show October 4, 2025 5:30 PM - 10:00 PM
  console.log('\n--- EXPECTED vs ACTUAL ---');
  console.log('Expected: October 4, 2025 5:30 PM - 10:00 PM');
  console.log('Actual:  ', result.start.toLocaleString('en-US', { 
    timeZone: 'America/Los_Angeles',
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  }), '-', result.end.toLocaleString('en-US', { 
    timeZone: 'America/Los_Angeles',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  }));
  
  console.log('\n--- FLOATING TIME CHECK ---');
  console.log('Start local time:', result.start.toLocaleString('en-US', { 
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  }));
  console.log('End local time:', result.end.toLocaleString('en-US', { 
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  }));
  
  console.log('\n--- DATE COMPONENTS ---');
  console.log('Start - Year:', result.start.getFullYear(), 'Month:', result.start.getMonth() + 1, 'Date:', result.start.getDate(), 'Hours:', result.start.getHours(), 'Minutes:', result.start.getMinutes());
  console.log('End - Year:', result.end.getFullYear(), 'Month:', result.end.getMonth() + 1, 'Date:', result.end.getDate(), 'Hours:', result.end.getHours(), 'Minutes:', result.end.getMinutes());
} else {
  console.log('❌ PARSING FAILED');
}

