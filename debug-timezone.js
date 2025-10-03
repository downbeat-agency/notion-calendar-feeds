// Debug script to trace exactly what happens with timezone handling

// Copy the current createFloatingDate function exactly as it is
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
    // represents the correct local time. We do this by calculating the exact UTC time
    // that represents the local time we want.
    
    // First, create the date as local time to get the correct timezone offset
    const localDate = new Date(year, month, day, hours, minutes, 0, 0);
    
    // Get the timezone offset in milliseconds
    const timezoneOffset = localDate.getTimezoneOffset() * 60000;
    
    // Calculate the UTC time that represents this local time
    const utcTime = localDate.getTime() - timezoneOffset;
    
    // Create a new date from the calculated UTC time
    const date = new Date(utcTime);
    
    return date;
  } catch (e) {
    console.warn('Failed to create floating date:', dateTimeStr, e);
    return null;
  }
}

// Test with the exact input from your Notion data
console.log('=== DEBUGGING TIMEZONE ISSUE ===\n');

const notionInput = "October 4, 2025 5:30 PM";
console.log('1. Notion Input:', notionInput);

const parsedDate = createFloatingDate(notionInput);
console.log('2. After createFloatingDate():');
console.log('   - Date object:', parsedDate);
console.log('   - toString():', parsedDate.toString());
console.log('   - toISOString():', parsedDate.toISOString());
console.log('   - getUTCFullYear():', parsedDate.getUTCFullYear());
console.log('   - getUTCMonth():', parsedDate.getUTCMonth());
console.log('   - getUTCDate():', parsedDate.getUTCDate());
console.log('   - getUTCHours():', parsedDate.getUTCHours());
console.log('   - getUTCMinutes():', parsedDate.getUTCMinutes());

console.log('\n3. Local time components:');
console.log('   - getFullYear():', parsedDate.getFullYear());
console.log('   - getMonth():', parsedDate.getMonth());
console.log('   - getDate():', parsedDate.getDate());
console.log('   - getHours():', parsedDate.getHours());
console.log('   - getMinutes():', parsedDate.getMinutes());

console.log('\n4. Timezone offset:');
console.log('   - getTimezoneOffset():', parsedDate.getTimezoneOffset(), 'minutes');
console.log('   - Timezone offset in hours:', parsedDate.getTimezoneOffset() / 60);

console.log('\n5. What happens when ical-generator processes this:');
console.log('   - The Date object represents:', parsedDate.toString());
console.log('   - When converted to UTC for ICS:', parsedDate.toISOString());
console.log('   - This means the ICS will have DTSTART:', parsedDate.toISOString().replace(/[-:T]/g, '').split('.')[0]);

console.log('\n6. Analysis:');
console.log('   - Input was: October 4, 2025 5:30 PM');
console.log('   - Date object created as: October 4, 2025 5:30 PM LOCAL TIME');
console.log('   - But when converted to UTC:', parsedDate.toISOString());
console.log('   - The UTC time is:', parsedDate.getUTCDate(), 'days,', parsedDate.getUTCHours(), 'hours,', parsedDate.getUTCMinutes(), 'minutes');

// Let's also test what happens if we create the date differently
console.log('\n=== COMPARISON: Different approaches ===\n');

// Approach 1: Current approach (local time)
const localDate = new Date(2025, 9, 4, 17, 30, 0, 0); // October 4, 2025 5:30 PM local
console.log('Local approach:');
console.log('  - Date:', localDate.toString());
console.log('  - UTC:', localDate.toISOString());

// Approach 2: UTC approach
const utcDate = new Date(Date.UTC(2025, 9, 4, 17, 30, 0, 0)); // October 4, 2025 5:30 PM UTC
console.log('\nUTC approach:');
console.log('  - Date:', utcDate.toString());
console.log('  - UTC:', utcDate.toISOString());

// Approach 3: What if we want floating time?
console.log('\nFloating time analysis:');
console.log('  - For floating time, we want: October 4, 2025 5:30 PM in ANY timezone');
console.log('  - This means the ICS should show: DTSTART:20251004T173000 (no timezone)');
console.log('  - But our current approach creates a local time that gets converted to UTC');
