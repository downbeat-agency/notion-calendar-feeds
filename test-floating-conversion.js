// Test script to verify floating time conversion
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
      
      // Parse the dates (they already include timezone info)
      const startUTC = new Date(startStr.trim());
      const endUTC = new Date(endStr.trim());
      
      if (isNaN(startUTC.getTime()) || isNaN(endUTC.getTime())) {
        console.warn('Invalid date parsing:', { startStr, endStr });
        return null;
      }
      
      // The dates already have timezone info, so we just need to create floating times
      // that represent the local time without timezone conversion
      // Extract the local time components and create floating dates
      
      const startYear = startUTC.getFullYear();
      const startMonth = startUTC.getMonth();
      const startDate = startUTC.getDate();
      const startHours = startUTC.getHours();
      const startMinutes = startUTC.getMinutes();
      const startSeconds = startUTC.getSeconds();
      
      const endYear = endUTC.getFullYear();
      const endMonth = endUTC.getMonth();
      const endDate = endUTC.getDate();
      const endHours = endUTC.getHours();
      const endMinutes = endUTC.getMinutes();
      const endSeconds = endUTC.getSeconds();
      
      // Create floating dates (no timezone conversion)
      const start = new Date(startYear, startMonth, startDate, startHours, startMinutes, startSeconds);
      const end = new Date(endYear, endMonth, endDate, endHours, endMinutes, endSeconds);
      
      return { start, end };
    } catch (e) {
      console.warn('Failed to parse ISO date:', cleanStr, e);
      return null;
    }
  }
  
  return null;
}

console.log('=== TESTING FLOATING TIME CONVERSION ===\n');

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
} else {
  console.log('❌ PARSING FAILED');
}
