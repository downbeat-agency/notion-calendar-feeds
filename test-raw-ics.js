// Test script to generate raw ICS output and analyze floating time behavior
import ical from 'ical-generator';

// Test the current createFloatingDate function
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

// Test the original approach (before fix)
function createFloatingDateOriginal(dateTimeStr) {
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
    
    // Original approach: Local time
    const date = new Date(year, month, day, hours, minutes, 0, 0);
    return date;
  } catch (e) {
    console.warn('Failed to create floating date:', dateTimeStr, e);
    return null;
  }
}

// Test with Date.UTC approach
function createFloatingDateUTC(dateTimeStr) {
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
      
      return new Date(Date.UTC(year, month, day, hours, minutes, seconds));
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
    
    // UTC approach: Create date in UTC
    const date = new Date(Date.UTC(year, month, day, hours, minutes, 0, 0));
    return date;
  } catch (e) {
    console.warn('Failed to create floating date:', dateTimeStr, e);
    return null;
  }
}

console.log('=== TESTING DIFFERENT FLOATING TIME APPROACHES ===\n');

const testDate = "October 4, 2025 3:00 PM";
const testEndDate = "October 4, 2025 10:00 PM";

console.log('Input:', testDate, '→', testEndDate);
console.log('Expected ICS: DTSTART:20251004T150000, DTEND:20251004T220000\n');

// Test current approach (after fix)
console.log('1. CURRENT APPROACH (after fix):');
const currentStart = createFloatingDate(testDate);
const currentEnd = createFloatingDate(testEndDate);
console.log('  Start date:', currentStart);
console.log('  End date:', currentEnd);
console.log('  Start ISO:', currentStart.toISOString());
console.log('  End ISO:', currentEnd.toISOString());

const calendar1 = ical({ name: 'Current Approach' });
calendar1.createEvent({
  start: currentStart,
  end: currentEnd,
  summary: 'Pasadena Event (Gold Standard)',
  description: 'Test event',
  location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
  floating: true
});

console.log('  ICS DTSTART/DTEND:');
const ics1 = calendar1.toString();
const dtstart1 = ics1.match(/DTSTART:(\d{8}T\d{6})/);
const dtend1 = ics1.match(/DTEND:(\d{8}T\d{6})/);
console.log('  DTSTART:', dtstart1 ? dtstart1[1] : 'NOT FOUND');
console.log('  DTEND:', dtend1 ? dtend1[1] : 'NOT FOUND');

// Test original approach (before fix)
console.log('\n2. ORIGINAL APPROACH (before fix):');
const originalStart = createFloatingDateOriginal(testDate);
const originalEnd = createFloatingDateOriginal(testEndDate);
console.log('  Start date:', originalStart);
console.log('  End date:', originalEnd);
console.log('  Start ISO:', originalStart.toISOString());
console.log('  End ISO:', originalEnd.toISOString());

const calendar2 = ical({ name: 'Original Approach' });
calendar2.createEvent({
  start: originalStart,
  end: originalEnd,
  summary: 'Pasadena Event (Gold Standard)',
  description: 'Test event',
  location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
  floating: true
});

console.log('  ICS DTSTART/DTEND:');
const ics2 = calendar2.toString();
const dtstart2 = ics2.match(/DTSTART:(\d{8}T\d{6})/);
const dtend2 = ics2.match(/DTEND:(\d{8}T\d{6})/);
console.log('  DTSTART:', dtstart2 ? dtstart2[1] : 'NOT FOUND');
console.log('  DTEND:', dtend2 ? dtend2[1] : 'NOT FOUND');

// Test UTC approach
console.log('\n3. UTC APPROACH:');
const utcStart = createFloatingDateUTC(testDate);
const utcEnd = createFloatingDateUTC(testEndDate);
console.log('  Start date:', utcStart);
console.log('  End date:', utcEnd);
console.log('  Start ISO:', utcStart.toISOString());
console.log('  End ISO:', utcEnd.toISOString());

const calendar3 = ical({ name: 'UTC Approach' });
calendar3.createEvent({
  start: utcStart,
  end: utcEnd,
  summary: 'Pasadena Event (Gold Standard)',
  description: 'Test event',
  location: '1401 S Oak Knoll Ave Pasadena, CA 91106',
  floating: true
});

console.log('  ICS DTSTART/DTEND:');
const ics3 = calendar3.toString();
const dtstart3 = ics3.match(/DTSTART:(\d{8}T\d{6})/);
const dtend3 = ics3.match(/DTEND:(\d{8}T\d{6})/);
console.log('  DTSTART:', dtstart3 ? dtstart3[1] : 'NOT FOUND');
console.log('  DTEND:', dtend3 ? dtend3[1] : 'NOT FOUND');

console.log('\n=== FULL ICS OUTPUT (Current Approach) ===');
console.log(ics1);
