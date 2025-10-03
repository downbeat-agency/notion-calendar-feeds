// Test script to verify timezone fix
import ical from 'ical-generator';

// Copy the createFloatingDate function from the main code
function createFloatingDate(dateTimeStr) {
  if (!dateTimeStr) return null;
  
  try {
    // Parse the date string manually to avoid timezone conversion
    // Expected format: "October 4, 2025 3:00 PM" or "October 4, 2025 3:00 PM"
    const match = dateTimeStr.match(/([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s+(AM|PM)/i);
    if (!match) {
      // Fallback to regular Date parsing
      const date = new Date(dateTimeStr);
      if (isNaN(date.getTime())) return null;
      
      // Create a new Date object with the local components (no timezone conversion)
      const year = date.getFullYear();
      const month = date.getMonth();
      const day = date.getDate();
      const hours = date.getHours();
      const minutes = date.getMinutes();
      const seconds = date.getSeconds();
      
      return new Date(year, month, day, hours, minutes, seconds);
    }
    
    // Extract components from the match
    const monthName = match[1];
    const day = parseInt(match[2]);
    const year = parseInt(match[3]);
    let hours = parseInt(match[4]);
    const minutes = parseInt(match[5]);
    const period = match[6].toUpperCase();
    
    // Convert to 24-hour format
    if (period === 'PM' && hours !== 12) hours += 12;
    if (period === 'AM' && hours === 12) hours = 0;
    
    // Convert month name to number (0-based)
    const monthMap = {
      'january': 0, 'february': 1, 'march': 2, 'april': 3, 'may': 4, 'june': 5,
      'july': 6, 'august': 7, 'september': 8, 'october': 9, 'november': 10, 'december': 11
    };
    const month = monthMap[monthName.toLowerCase()];
    if (month === undefined) return null;
    
    // Create a new Date object with these exact components (no timezone conversion)
    // For floating times, we need to create a date that when converted to UTC
    // for the ICS format, represents the correct local time.
    // Since we're in Pacific time (UTC-7 or UTC-8), we need to add the offset
    // to get the correct UTC time that represents our local time.
    
    // Create the date as if it's in UTC
    const utcDate = new Date(Date.UTC(year, month, day, hours, minutes, 0, 0));
    
    // Get the current timezone offset (in minutes, positive for west of UTC)
    const timezoneOffset = new Date().getTimezoneOffset();
    
    // Adjust the UTC date by the timezone offset to get the correct local time
    const adjustedDate = new Date(utcDate.getTime() + (timezoneOffset * 60000));
    
    return adjustedDate;
  } catch (e) {
    console.warn('Failed to create floating date:', dateTimeStr, e);
    return null;
  }
}

// Test the function with the problematic date
console.log('Testing timezone fix...\n');

const testDate = "October 4, 2025 5:30 PM";
const parsedDate = createFloatingDate(testDate);

console.log('Input:', testDate);
console.log('Parsed date:', parsedDate);
console.log('Date string:', parsedDate.toString());
console.log('ISO string:', parsedDate.toISOString());
console.log('Local date string:', parsedDate.toLocaleDateString());
console.log('Local time string:', parsedDate.toLocaleTimeString());

// Test creating an ICS event
const calendar = ical({ name: 'Timezone Test Calendar' });

calendar.createEvent({
  start: parsedDate,
  end: new Date(parsedDate.getTime() + 4.5 * 60 * 60 * 1000), // 4.5 hours later
  summary: 'Pasadena Event (Gold Standard)',
  description: 'Test event to verify timezone fix',
  location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
  floating: true
});

console.log('\nGenerated ICS:');
console.log(calendar.toString());
