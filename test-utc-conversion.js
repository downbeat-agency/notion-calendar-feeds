// Test script to verify UTC to Pacific conversion
import ical from 'ical-generator';

// Copy the getPacificOffset function
function getPacificOffset(date) {
  // Check if the date is in DST period
  if (isDSTDate(date)) {
    return -7; // PDT is UTC-7
  } else {
    return -8; // PST is UTC-8
  }
}

function isDSTDate(date) {
  const year = date.getFullYear();
  
  // DST starts on the second Sunday of March
  const marchFirst = new Date(year, 2, 1);
  const marchFirstDay = marchFirst.getDay();
  const secondSunday = 1 + (7 - marchFirstDay) % 7 + 7;
  const dstStart = new Date(year, 2, secondSunday, 2, 0, 0);
  
  // DST ends on the first Sunday of November
  const novemberFirst = new Date(year, 10, 1);
  const novemberFirstDay = novemberFirst.getDay();
  const firstSunday = 1 + (7 - novemberFirstDay) % 7;
  const dstEnd = new Date(year, 10, firstSunday, 2, 0, 0);
  
  return date >= dstStart && date < dstEnd;
}

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
      
      // Parse the UTC dates
      const startUTC = new Date(startStr.trim());
      const endUTC = new Date(endStr.trim());
      
      if (isNaN(startUTC.getTime()) || isNaN(endUTC.getTime())) {
        console.warn('Invalid date parsing:', { startStr, endStr });
        return null;
      }
      
      // Convert UTC to Pacific Time for floating events
      const pacificOffset = getPacificOffset(startUTC);
      
      // Create floating dates by adding the Pacific offset to UTC
      const start = new Date(startUTC.getTime() + (pacificOffset * 60 * 1000));
      const end = new Date(endUTC.getTime() + (pacificOffset * 60 * 1000));
      
      return { start, end };
    } catch (e) {
      console.warn('Failed to parse ISO date:', cleanStr, e);
      return null;
    }
  }
  
  return null;
}

console.log('=== TESTING UTC TO PACIFIC CONVERSION ===\n');

// Test with the Pasadena Event data
const testDate = "2025-10-04T17:30:00-07:00/2025-10-04T22:00:00-07:00";
console.log('Input (from your new data):', testDate);

const result = parseUnifiedDateTime(testDate);
console.log('Result:', result);

if (result) {
  console.log('\n--- CONVERSION RESULTS ---');
  console.log('Start (Pacific):', result.start.toString());
  console.log('End (Pacific):', result.end.toString());
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
} else {
  console.log('❌ PARSING FAILED');
}

