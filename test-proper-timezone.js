// Test script to verify proper timezone handling without stringified conversion
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
      
      // The dates already have timezone info, so we need to extract the local Pacific time
      // and create floating dates that represent that local time
      
      // Parse the timezone offset from the ISO string
      const startOffsetMatch = startStr.match(/([+-]\d{2}):?(\d{2})$/);
      const endOffsetMatch = endStr.match(/([+-]\d{2}):?(\d{2})$/);
      
      let startOffsetHours = 0;
      let endOffsetHours = 0;
      
      if (startOffsetMatch) {
        startOffsetHours = parseInt(startOffsetMatch[1] + startOffsetMatch[2]) / 100;
      }
      if (endOffsetMatch) {
        endOffsetHours = parseInt(endOffsetMatch[1] + endOffsetMatch[2]) / 100;
      }
      
      // Get the local time by adjusting for the timezone offset
      const startLocalTime = new Date(startUTC.getTime() - (startOffsetHours * 60 * 60 * 1000));
      const endLocalTime = new Date(endUTC.getTime() - (endOffsetHours * 60 * 60 * 1000));
      
      // Extract the local time components
      const startYear = startLocalTime.getUTCFullYear();
      const startMonth = startLocalTime.getUTCMonth();
      const startDate = startLocalTime.getUTCDate();
      const startHours = startLocalTime.getUTCHours();
      const startMinutes = startLocalTime.getUTCMinutes();
      const startSeconds = startLocalTime.getUTCSeconds();
      
      const endYear = endLocalTime.getUTCFullYear();
      const endMonth = endLocalTime.getUTCMonth();
      const endDate = endLocalTime.getUTCDate();
      const endHours = endLocalTime.getUTCHours();
      const endMinutes = endLocalTime.getUTCMinutes();
      const endSeconds = endLocalTime.getUTCSeconds();
      
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

console.log('=== TESTING PROPER TIMEZONE HANDLING ===\n');

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

