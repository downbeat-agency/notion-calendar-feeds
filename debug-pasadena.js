// Debug script to test the exact Pasadena Event data
import ical from 'ical-generator';

// Copy the current createFloatingDate function
function createFloatingDate(dateTimeStr) {
  if (!dateTimeStr) return null;
  
  try {
    const match = dateTimeStr.match(/([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s+(AM|PM)/i);
    if (!match) {
      const date = new Date(dateTimeStr);
      if (isNaN(date.getTime())) return null;
      
      const year = date.getFullYear();
      const month = date.getMonth();
      const day = date.getDate();
      const hours = date.getHours();
      const minutes = date.getMinutes();
      const seconds = date.getSeconds();
      
      return new Date(year, month, day, hours, minutes, seconds);
    }
    
    const monthName = match[1];
    const day = parseInt(match[2]);
    const year = parseInt(match[3]);
    let hours = parseInt(match[4]);
    const minutes = parseInt(match[5]);
    const period = match[6].toUpperCase();
    
    if (period === 'PM' && hours !== 12) hours += 12;
    if (period === 'AM' && hours === 12) hours = 0;
    
    const monthMap = {
      'january': 0, 'february': 1, 'march': 2, 'april': 3, 'may': 4, 'june': 5,
      'july': 6, 'august': 7, 'september': 8, 'october': 9, 'november': 10, 'december': 11
    };
    const month = monthMap[monthName.toLowerCase()];
    if (month === undefined) return null;
    
    // Current fix: Manual UTC calculation
    const localDate = new Date(year, month, day, hours, minutes, 0, 0);
    const timezoneOffset = localDate.getTimezoneOffset() * 60000;
    const utcTime = localDate.getTime() - timezoneOffset;
    const date = new Date(utcTime);
    
    return date;
  } catch (e) {
    console.warn('Failed to create floating date:', dateTimeStr, e);
    return null;
  }
}

// Copy the parseUnifiedDateTime function
function parseUnifiedDateTime(dateTimeStr) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  
  if (cleanStr.startsWith('@')) {
    const match = cleanStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s+→\s+(.+)/i);
    if (match) {
      const dateStr = match[1].trim();
      const startTimeStr = match[2].trim();
      const endPart = match[3].trim();
      
      let endTimeStr, endDateStr;
      const endMatch = endPart.match(/(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
      if (endMatch && endMatch[1].toLowerCase().includes(',')) {
        endDateStr = endMatch[1].trim();
        endTimeStr = endMatch[2].trim();
      } else {
        endDateStr = dateStr;
        endTimeStr = endPart;
      }
      
      try {
        const startDate = createFloatingDate(`${dateStr} ${startTimeStr}`);
        const endDate = createFloatingDate(`${endDateStr} ${endTimeStr}`);
        
        if (startDate && endDate) {
          return {
            start: startDate,
            end: endDate
          };
        }
      } catch (e) {
        console.warn('Failed to parse unified date format:', cleanStr, e);
      }
    }
  }
  
  return null;
}

console.log('=== DEBUGGING PASADENA EVENT ===\n');

// Test with the exact Pasadena Event data
const pasadenaEventDate = "@October 4, 2025 3:00 PM → 10:00 PM";
console.log('Notion event_date:', pasadenaEventDate);

const eventTimes = parseUnifiedDateTime(pasadenaEventDate);
console.log('Parsed eventTimes:', eventTimes);

if (eventTimes) {
  console.log('Start date:', eventTimes.start);
  console.log('End date:', eventTimes.end);
  console.log('Start ISO:', eventTimes.start.toISOString());
  console.log('End ISO:', eventTimes.end.toISOString());
  
  // Generate ICS
  const calendar = ical({ name: 'Pasadena Event Test' });
  calendar.createEvent({
    start: eventTimes.start,
    end: eventTimes.end,
    summary: 'Pasadena Event (Gold Standard)',
    description: 'Test event',
    location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
    floating: true
  });
  
  console.log('\n=== GENERATED ICS ===');
  const ics = calendar.toString();
  console.log(ics);
  
  // Extract DTSTART and DTEND
  const dtstartMatch = ics.match(/DTSTART:(\d{8}T\d{6})/);
  const dtendMatch = ics.match(/DTEND:(\d{8}T\d{6})/);
  console.log('\n=== ICS ANALYSIS ===');
  console.log('DTSTART:', dtstartMatch ? dtstartMatch[1] : 'NOT FOUND');
  console.log('DTEND:', dtendMatch ? dtendMatch[1] : 'NOT FOUND');
  console.log('Expected: DTSTART:20251004T150000, DTEND:20251004T220000');
} else {
  console.log('ERROR: Failed to parse event times');
}

