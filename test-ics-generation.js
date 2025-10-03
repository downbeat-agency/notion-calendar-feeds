// Test full ICS generation with the fixed timezone handling
import ical from 'ical-generator';

// Copy the fixed createFloatingDate function
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
    
    // FIXED: Manual UTC calculation for floating times
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

// Test with the problematic event
console.log('=== TESTING ICS GENERATION WITH FIXED TIMEZONE ===\n');

const testDate = "October 4, 2025 5:30 PM";
const parsedDate = createFloatingDate(testDate);

console.log('Input:', testDate);
console.log('Parsed date:', parsedDate);
console.log('Date string:', parsedDate.toString());
console.log('ISO string:', parsedDate.toISOString());

// Create end date (4.5 hours later)
const endDate = new Date(parsedDate.getTime() + 4.5 * 60 * 60 * 1000);

console.log('\nEnd date:', endDate.toISOString());

// Generate ICS calendar
const calendar = ical({ name: 'Timezone Test Calendar' });

calendar.createEvent({
  start: parsedDate,
  end: endDate,
  summary: 'Pasadena Event (Gold Standard)',
  description: 'Test event to verify timezone fix',
  location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
  floating: true
});

console.log('\n=== GENERATED ICS ===');
console.log(calendar.toString());

// Extract and analyze the DTSTART line
const icsContent = calendar.toString();
const dtstartMatch = icsContent.match(/DTSTART:(\d{8}T\d{6})/);
if (dtstartMatch) {
  const dtstart = dtstartMatch[1];
  console.log('\n=== ANALYSIS ===');
  console.log('DTSTART in ICS:', dtstart);
  console.log('Expected: 20251004T173000 (October 4, 2025 17:30:00)');
  console.log('Match:', dtstart === '20251004T173000' ? '✅ CORRECT' : '❌ INCORRECT');
}
