import 'dotenv/config';
import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';
import { createClient } from 'redis';
import path from 'path';
import axios from 'axios';

// Server refresh - October 1, 2025
// Updated with event_personnel field support - October 8, 2025
// Retry deployment after network issues resolved
// Force deployment - testing event_personnel integration

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ 
  auth: process.env.NOTION_API_KEY,
  timeoutMs: 90000 // 90 seconds - longer than Railway's 60s timeout to handle slow Notion responses
});

// Redis client setup
let redis = null;
let cacheEnabled = false;

try {
  if (process.env.REDIS_URL) {
    redis = createClient({
      url: process.env.REDIS_URL
    });

    redis.on('error', (err) => {
      console.error('Redis Client Error:', err.message);
      cacheEnabled = false;
    });
    
    redis.on('connect', () => {
      console.log('✅ Redis connected successfully');
      cacheEnabled = true;
    });

    // Connect to Redis
    await redis.connect();
  } else {
    console.warn('⚠️  REDIS_URL not configured - caching disabled');
  }
} catch (err) {
  console.error('Failed to connect to Redis:', err.message);
  console.warn('⚠️  Continuing without cache');
  redis = null;
  cacheEnabled = false;
}

// FlightAware AeroAPI configuration
const FLIGHTAWARE_API_KEY = process.env.FLIGHTAWARE_API_KEY;
const FLIGHTAWARE_BASE_URL = 'https://aeroapi.flightaware.com/aeroapi';

// FlightAware API helper functions
async function fetchFlightStatus(airline, flightNumber, departureDate) {
  if (!FLIGHTAWARE_API_KEY) {
    throw new Error('FlightAware API key not configured');
  }

  // Create flight ident (e.g., "DL915")
  const ident = `${airline}${flightNumber}`.replace(/\s+/g, '');
  
  // Create date range for the flight (24 hours before and after departure)
  const depDate = new Date(departureDate);
  const startDate = new Date(depDate.getTime() - 24 * 60 * 60 * 1000);
  const endDate = new Date(depDate.getTime() + 24 * 60 * 60 * 1000);
  
  const start = startDate.toISOString().split('T')[0];
  const end = endDate.toISOString().split('T')[0];

  try {
    const response = await axios.get(`${FLIGHTAWARE_BASE_URL}/flights/${ident}`, {
      headers: {
        'x-apikey': FLIGHTAWARE_API_KEY
      },
      params: {
        start: start,
        end: end,
        max_pages: 1
      },
      timeout: 10000 // 10 second timeout
    });

    if (response.data && response.data.flights && response.data.flights.length > 0) {
      // Find the flight closest to our departure date
      const flights = response.data.flights;
      const targetFlight = flights.find(flight => {
        const flightDate = new Date(flight.scheduled_out);
        return Math.abs(flightDate - depDate) < 24 * 60 * 60 * 1000; // Within 24 hours
      }) || flights[0]; // Fallback to first flight

      return {
        ident: targetFlight.ident,
        status: targetFlight.status,
        scheduled_out: targetFlight.scheduled_out,
        estimated_out: targetFlight.estimated_out,
        actual_out: targetFlight.actual_out,
        scheduled_in: targetFlight.scheduled_in,
        estimated_in: targetFlight.estimated_in,
        actual_in: targetFlight.actual_in,
        origin: targetFlight.origin,
        destination: targetFlight.destination,
        origin_gate: targetFlight.origin_gate,
        destination_gate: targetFlight.destination_gate,
        origin_terminal: targetFlight.origin_terminal,
        destination_terminal: targetFlight.destination_terminal,
        baggage_claim: targetFlight.baggage_claim,
        delay: targetFlight.delay
      };
    }
    
    return null;
  } catch (error) {
    console.error('FlightAware API error:', error.response?.status, error.response?.data || error.message);
    throw error;
  }
}

// Serve static files from public directory
app.use(express.static('public'));

// Use environment variable for Personnel database ID
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;
const ADMIN_CALENDAR_PAGE_ID = process.env.ADMIN_CALENDAR_PAGE_ID;
const TRAVEL_CALENDAR_PAGE_ID = process.env.TRAVEL_CALENDAR_PAGE_ID;
const BLOCKOUT_CALENDAR_PAGE_ID = process.env.BLOCKOUT_CALENDAR_PAGE_ID;

// Cache TTL in seconds (8 minutes for 5-minute background refresh cycle)
const CACHE_TTL = parseInt(process.env.CACHE_TTL) || 480;

// Helper function to generate flight countdown URL
function generateFlightCountdownUrl(flightData, direction = 'departure') {
  const baseUrl = (process.env.BASE_URL || 'https://calendar.downbeat.agency').replace(/\/$/, '');
  
  // Always use URL params since we already have all the flight data
  const params = new URLSearchParams({
    flight: flightData.flightNumber || 'N/A',
    departure: flightData.departureTime,
    airline: flightData.airline || 'N/A',
    route: flightData.route || 'N/A',
    confirmation: flightData.confirmation || 'N/A',
    departureCode: flightData.departureCode || 'N/A',
    arrivalCode: flightData.arrivalCode || 'N/A',
    departureName: flightData.departureName || 'N/A',
    arrivalName: flightData.arrivalName || 'N/A'
  });
  return `${baseUrl}/flight-countdown-modern.html?${params.toString()}`;
}

// Helper function to get appropriate alarms for each event type
function getAlarmsForEvent(eventType, eventTitle = '') {
  // Skip alarms for OOO events
  if (eventTitle && eventTitle.includes('⛔️') && eventTitle.toUpperCase().includes('OOO')) {
    return [];
  }
  
  const alarmConfigs = {
    // FLIGHTS: 3 hours before
    'flight_departure': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_return': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_departure_layover': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_return_layover': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    
    // MAIN EVENTS: 1 hour before
    'main_event': [
      { type: 'display', trigger: 3600 }    // 1 hour
    ],
    
    // REHEARSALS: 24 hours before
    'rehearsal': [
      { type: 'display', trigger: 86400 }   // 24 hours
    ],
    
    // HOTELS: 4 hours before check-in
    'hotel': [
      { type: 'display', trigger: 14400 }   // 4 hours
    ],
    
    // TRANSPORTATION: 45 mins for pickup/meetup ONLY
    'ground_transport_pickup': [
      { type: 'display', trigger: 2700 }    // 45 minutes
    ],
    'ground_transport_meeting': [
      { type: 'display', trigger: 2700 }    // 45 minutes
    ],
    'ground_transport_dropoff': [],         // NO ALARM
    'ground_transport': [],                 // NO ALARM
    
    // TEAM CALENDAR: None
    'team_calendar': []                     // NO ALARM
  };
  
  return alarmConfigs[eventType] || [];
}

// Helper function to convert timezone-aware ISO 8601 to Pacific time (updated v2)
function convertToPacific(isoString) {
  if (!isoString) return null;
  
  try {
    // Parse the ISO string with timezone offset (e.g., -07:00, -08:00)
    const date = new Date(isoString);
    
    if (isNaN(date.getTime())) {
      console.warn('Invalid date string:', isoString);
      return null;
    }
    
    // The date is already correctly parsed with timezone information
    // Just return it as-is since it's already in the correct timezone
    return date;
  } catch (e) {
    console.warn('Failed to parse ISO date:', isoString, e);
    return null;
  }
}

// Helper function to determine Pacific timezone offset
function getPacificOffset(date) {
  // Simple check: March 9 to November 2, 2025 should be PDT (UTC-7)
  // This is a simplified check - in production you'd want a proper timezone library
  const month = date.getMonth() + 1; // getMonth() is 0-based
  const day = date.getDate();
  
  if (month > 3 && month < 11) return '-07:00'; // PDT
  if (month === 3 && day >= 9) return '-07:00'; // PDT
  if (month === 11 && day <= 2) return '-07:00'; // PDT
  return '-08:00'; // PST
}

// Helper function to check if a date is in DST period
function isDSTDate(date) {
  const year = date.getFullYear();
  const month = date.getMonth();
  const day = date.getDate();
  
  // DST starts second Sunday in March at 2 AM
  const marchFirst = new Date(year, 2, 1); // March 1
  const dstStart = new Date(year, 2, 1 + (7 - marchFirst.getDay() + 7) % 7 + 7); // Second Sunday
  
  // DST ends first Sunday in November at 2 AM
  const novFirst = new Date(year, 10, 1); // November 1
  const dstEnd = new Date(year, 10, 1 + (7 - novFirst.getDay()) % 7); // First Sunday
  
  const checkDate = new Date(year, month, day);
  return checkDate >= dstStart && checkDate < dstEnd;
}

// Helper function to format ISO timestamp to readable time (e.g., "1:30 PM")
// The calltime is stored as Pacific time but tagged as UTC, so we use the hour value directly
function formatCallTime(isoTimestamp) {
  if (!isoTimestamp || typeof isoTimestamp !== 'string') {
    return isoTimestamp;
  }
  
  // Try to parse as ISO timestamp
  const match = isoTimestamp.match(/T(\d{2}):(\d{2})/);
  if (!match) {
    return isoTimestamp; // Return as-is if not ISO format
  }
  
  let hours = parseInt(match[1], 10);
  const minutes = match[2];
  
  // Convert to 12-hour format
  const period = hours >= 12 ? 'PM' : 'AM';
  if (hours === 0) {
    hours = 12;
  } else if (hours > 12) {
    hours -= 12;
  }
  
  // Format with or without minutes
  if (minutes === '00') {
    return `${hours} ${period}`;
  }
  return `${hours}:${minutes} ${period}`;
}

// Helper function to retry Notion API calls with exponential backoff
async function retryNotionCall(apiCall, maxRetries = 3) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await apiCall();
    } catch (error) {
      lastError = error;
      const isRetryable = error.code === 'notionhq_client_request_timeout' || 
                         error.status === 504 || 
                         error.message?.includes('504') ||
                         error.message?.includes('timeout') ||
                         error.message?.includes('Request to Notion API failed');
      
      if (isRetryable && attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000); // Exponential backoff, max 5s
        console.log(`⚠️  Notion API timeout (attempt ${attempt}/${maxRetries}), retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      
      // Not retryable or max retries reached
      throw error;
    }
  }
  
  throw lastError;
}

// Helper function to get calendar data from Calendar Data database
async function getCalendarDataFromDatabase(personId) {
  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
  }

  // Query Calendar Data database for events related to this person
  // Use page_size: 1 since we only expect one result per person
  const response = await retryNotionCall(() => 
    notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      page_size: 1, // Optimize: only fetch one result
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    })
  );
  
  return processCalendarDataResponse(response);
}

// Helper function to process the response after successful query
function processCalendarDataResponse(response) {
  if (response.results.length === 0) {
    return null;
  }

  const calendarData = response.results[0].properties;

  
  // Parse all the JSON strings with better error handling
  let events, flights, transportation, hotels, rehearsals, teamCalendar;
  
  // DEBUG: Log raw formula string before parsing
  const rawFormulaString = calendarData.Events?.formula?.string || '';
  if (rawFormulaString.includes('Pacific Palisades') || rawFormulaString.includes('2025-11-15')) {
    console.log('[DEBUG FORMULA OUTPUT] Raw formula string length:', rawFormulaString.length);
    // Find the calltime value in the raw string for 11/15 event
    const calltimeMatch = rawFormulaString.match(/"calltime":"([^"]*2025-11-15[^"]*)"/);
    if (calltimeMatch) {
      console.log('[DEBUG FORMULA OUTPUT] Raw calltime from formula string:', calltimeMatch[1]);
    }
  }
  
  try {
    events = JSON.parse(rawFormulaString || '[]');
    // DEBUG: Log raw calltime values from database for 11/15 event
    if (events && Array.isArray(events)) {
      events.forEach(event => {
        if (event.event_name && event.event_name.includes('Pacific Palisades')) {
          console.log('[DEBUG RAW FROM DB] Event:', event.event_name);
          console.log('[DEBUG RAW FROM DB] Raw calltime from JSON:', event.calltime);
          console.log('[DEBUG RAW FROM DB] Raw calltime type:', typeof event.calltime);
          console.log('[DEBUG RAW FROM DB] Raw event_date from JSON:', event.event_date);
          console.log('[DEBUG RAW FROM DB] Full event JSON:', JSON.stringify(event, null, 2));
        }
      });
    }
  } catch (e) {
    console.error('Error parsing Events JSON:', calendarData.Events?.formula?.string?.substring(0, 100));
    throw new Error(`Events JSON parse error: ${e.message}`);
  }
  
  try {
    flights = JSON.parse(calendarData.Flights?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Flights JSON:', calendarData.Flights?.formula?.string?.substring(0, 100));
    throw new Error(`Flights JSON parse error: ${e.message}`);
  }
  
  try {
    transportation = JSON.parse(calendarData.Transportation?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Transportation JSON:', calendarData.Transportation?.formula?.string?.substring(0, 100));
    throw new Error(`Transportation JSON parse error: ${e.message}`);
  }
  
  try {
    hotels = JSON.parse(calendarData.Hotels?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Hotels JSON:', calendarData.Hotels?.formula?.string?.substring(0, 100));
    throw new Error(`Hotels JSON parse error: ${e.message}`);
  }
  
  try {
    rehearsals = JSON.parse(calendarData.Rehearsals?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Rehearsals JSON:', calendarData.Rehearsals?.formula?.string?.substring(0, 100));
    throw new Error(`Rehearsals JSON parse error: ${e.message}`);
  }
  
  try {
    teamCalendar = JSON.parse(calendarData['Team Calendar']?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Team Calendar JSON:', calendarData['Team Calendar']?.formula?.string?.substring(0, 100));
    throw new Error(`Team Calendar JSON parse error: ${e.message}`);
  }

  // Normalize Team Calendar keys (handle variations like DCOS vs dcos, Title vs title, etc.)
  teamCalendar = teamCalendar.map(original => {
    const normalized = { ...original };

    Object.keys(original || {}).forEach(key => {
      const normalizedKey = key.toLowerCase().trim();
      const value = original[key];

      switch (normalizedKey) {
        case 'title':
          if (!normalized.title && typeof value === 'string') {
            normalized.title = value;
          }
          break;
        case 'address':
        case 'location':
          if (!normalized.address && typeof value === 'string') {
            normalized.address = value;
          }
          break;
        case 'notion_link':
        case 'notionlink':
        case 'link':
          if (!normalized.notion_link && typeof value === 'string') {
            normalized.notion_link = value;
          }
          break;
        case 'notes':
          if (normalized.notes === undefined) {
            normalized.notes = value ?? '';
          }
          break;
        case 'dcos':
        case 'dcos_text':
          if (!normalized.dcos && typeof value === 'string') {
            normalized.dcos = value;
          }
          break;
        case 'date':
        case 'date_range':
          if (!normalized.date && typeof value === 'string') {
            normalized.date = value;
          }
          break;
        default:
          break;
      }
    });

    normalized.notes = normalized.notes ?? '';
    normalized.dcos = normalized.dcos ?? '';

    return normalized;
  });

  // Transform into the same format as the old system
  // Return events with shared flights, hotels, rehearsals, and transportation
  return {
    personName: 'Unknown', // Will be updated by the calling function
    events: events.map(event => ({
      event_name: event.event_name,
      event_date: event.event_date,
      band: event.band,
      calltime: event.calltime,
      gear_checklist: event.gear_checklist,
      event_personnel: event.event_personnel,
      general_info: event.general_info,
      venue: event.venue,
      venue_address: event.venue_address,
      notion_url: event.notion_url,
      pay_total: event.pay_total,
      position: event.position,
      assignments: event.assignments,
      // Don't nest these - they will be added at the top level
      flights: [],
      rehearsals: [],
      hotels: [],
      ground_transport: []
    })),
    // Add shared data at the top level
    flights: flights,
    rehearsals: rehearsals,
    hotels: hotels,
    ground_transport: transportation,
    team_calendar: teamCalendar
  };
}

// Helper function to parse @ format dates (for flights, rehearsals, hotels, transport)
function parseUnifiedDateTime(dateTimeStr) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  // Clean up the string
  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  
  // Check if it's the unified format with @
  if (cleanStr.startsWith('@')) {
    // First, try to match date-only format (for hotels): "@November 1, 2025 → November 2, 2025"
    const dateOnlyMatch = cleanStr.match(/@([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+→\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})/i);
    if (dateOnlyMatch) {
      try {
        const startDateStr = dateOnlyMatch[1].trim();
        const endDateStr = dateOnlyMatch[2].trim();
        
        // Parse dates and set to midnight (for all-day events)
        const startDate = new Date(startDateStr);
        const endDate = new Date(endDateStr);
        
        // Add Pacific offset for floating times
        const isDST = isDSTDate(startDate);
        const offsetHours = isDST ? 7 : 8;
        
        startDate.setHours(startDate.getHours() + offsetHours);
        endDate.setHours(endDate.getHours() + offsetHours);
        
        if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          return {
            start: startDate,
            end: endDate
          };
        }
      } catch (e) {
        console.warn('Failed to parse date-only format:', cleanStr, e);
      }
    }
    
    // Try to match format with times
    const match = cleanStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s+→\s+(.+)/i);
    if (match) {
      const dateStr = match[1].trim();
      const startTimeStr = match[2].trim();
      const endPart = match[3].trim();
      
      // Check if end part has a date (for multi-day events)
      let endTimeStr, endDateStr;
      const endMatch = endPart.match(/(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
      if (endMatch && endMatch[1].toLowerCase().includes(',')) {
        // Multi-day format: "November 9, 2025 1:00 AM"
        endDateStr = endMatch[1].trim();
        endTimeStr = endMatch[2].trim();
      } else {
        // Same day format: "12:00 PM"
        endDateStr = dateStr;
        endTimeStr = endPart;
      }
      
      try {
        // Parse dates as Pacific time and add offset to create floating times
        const startDate = new Date(`${dateStr} ${startTimeStr}`);
        const endDate = new Date(`${endDateStr} ${endTimeStr}`);
        
        // Add Pacific offset to create floating times that display correctly
        // DST: +7 hours (PDT), Standard: +8 hours (PST)
        const isDST = isDSTDate(startDate);
        const offsetHours = isDST ? 7 : 8;
        
        startDate.setHours(startDate.getHours() + offsetHours);
        endDate.setHours(endDate.getHours() + offsetHours);
        
        if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          // Final validation: ensure start is before end after conversion
          // This catches cases where multi-day events might have incorrect ordering
          let finalStart = startDate;
          let finalEnd = endDate;
          
          if (startDate.getTime() > endDate.getTime()) {
            console.warn(`[parseUnifiedDateTime @ format] Start > End after conversion. Swapping dates. Original: ${cleanStr}`);
            // Swap them - this handles edge cases with multi-day events
            finalStart = endDate;
            finalEnd = startDate;
          }
          
          return {
            start: finalStart,
            end: finalEnd
          };
        }
      } catch (e) {
        console.warn('Failed to parse unified date format:', cleanStr, e);
      }
    }
    
    // Fallback: try to parse as single date without end time
    const singleMatch = cleanStr.match(/@(.+)/);
    if (singleMatch) {
      try {
        const dateStr = singleMatch[1].trim();
        const date = new Date(dateStr);
        
        // Add Pacific offset for floating times
        const isDST = isDSTDate(date);
        const offsetHours = isDST ? 7 : 8;
        date.setHours(date.getHours() + offsetHours);
        
        if (!isNaN(date.getTime())) {
          return {
            start: date,
            end: date
          };
        }
      } catch (e) {
        console.warn('Failed to parse single date format:', cleanStr, e);
      }
    }
  }
  
  // Special handling for date range format: "2025-08-26T15:30:00+00:00/2025-09-14T06:00:00+00:00"
  if (cleanStr.includes('/')) {
    try {
      const parts = cleanStr.split('/');
      const firstStr = parts[0].trim();
      const secondStr = parts[1].trim();
      
      // Parse both dates - trust the order in the string (first = start, second = end)
      // Don't swap based on UTC comparison, as the database may have times that cross midnight
      // in a way that makes UTC comparison misleading
      let actualStartDate = new Date(firstStr);
      let actualEndDate = new Date(secondStr);
      const actualStartStr = firstStr;
      const actualEndStr = secondStr;
      
      if (!isNaN(actualStartDate.getTime()) && !isNaN(actualEndDate.getTime())) {
        
        // Check if these are UTC timestamps and convert to Pacific floating time
        const isUTCStart = actualStartStr.includes('T') && (actualStartStr.includes('Z') || actualStartStr.includes('+00:00'));
        const isUTCEnd = actualEndStr.includes('T') && (actualEndStr.includes('Z') || actualEndStr.includes('+00:00'));
        
        // Check if this appears to be an all-day event (midnight UTC times)
        // All-day events typically have times at 00:00:00 UTC
        const isAllDayStart = isUTCStart && actualStartStr.match(/T00:00:00/);
        const isAllDayEnd = isUTCEnd && actualEndStr.match(/T00:00:00/);
        
        if (isAllDayStart && isAllDayEnd) {
          // For all-day events, extract date components and create dates at midnight Pacific
          // This prevents date shifting when converting from UTC
          const startYear = actualStartDate.getUTCFullYear();
          const startMonth = actualStartDate.getUTCMonth();
          const startDay = actualStartDate.getUTCDate();
          
          const endYear = actualEndDate.getUTCFullYear();
          const endMonth = actualEndDate.getUTCMonth();
          const endDay = actualEndDate.getUTCDate();
          
          // Create new dates at midnight Pacific time (floating, no timezone)
          const pacificStart = new Date(startYear, startMonth, startDay, 0, 0, 0);
          const pacificEnd = new Date(endYear, endMonth, endDay, 0, 0, 0);
          
          return {
            start: pacificStart,
            end: pacificEnd
          };
        }
        
        // CROSS-MIDNIGHT DETECTION: Check if this is a cross-midnight event where Notion
        // stored the START time as Pacific time (incorrectly tagged as UTC)
        // Detection: Both timestamps on same UTC day, but first hour > second hour
        const sameUTCDay = actualStartDate.getUTCFullYear() === actualEndDate.getUTCFullYear() &&
                          actualStartDate.getUTCMonth() === actualEndDate.getUTCMonth() &&
                          actualStartDate.getUTCDate() === actualEndDate.getUTCDate();
        const startHourGreater = actualStartDate.getUTCHours() > actualEndDate.getUTCHours();
        const isCrossMidnightEvent = sameUTCDay && startHourGreater;
        
        // For timed events, convert UTC to Pacific floating time
        if (isUTCStart) {
          const isDST = isDSTDate(actualStartDate);
          const offsetHours = isDST ? 7 : 8;
          // Extract UTC components
          const year = actualStartDate.getUTCFullYear();
          const month = actualStartDate.getUTCMonth();
          const day = actualStartDate.getUTCDate();
          const originalUTCHours = actualStartDate.getUTCHours();
          const minutes = actualStartDate.getUTCMinutes();
          const seconds = actualStartDate.getUTCSeconds();
          
          // For cross-midnight events, the START time is already in Pacific (incorrectly tagged as UTC)
          // So we should NOT subtract the offset - just use the hour value directly
          // For cross-midnight events, the START time is already in Pacific (incorrectly tagged as UTC)
          // So we should NOT subtract the offset - just use the hour value directly
          let hours;
          if (isCrossMidnightEvent) {
            hours = originalUTCHours; // Use directly as Pacific time
          } else {
            hours = originalUTCHours - offsetHours;
          }
          
          // Handle hour underflow (if subtracting offset makes hours negative, go to previous day)
          if (hours < 0) {
            hours += 24;
            // Use Date constructor which handles month/day boundaries correctly
            actualStartDate = new Date(year, month, day - 1, hours, minutes, seconds);
          } else {
            actualStartDate = new Date(year, month, day, hours, minutes, seconds);
          }
        }
        
        if (isUTCEnd) {
          const isDST = isDSTDate(actualEndDate);
          const offsetHours = isDST ? 7 : 8;
          // Extract UTC components and convert to Pacific time
          const year = actualEndDate.getUTCFullYear();
          const month = actualEndDate.getUTCMonth();
          const day = actualEndDate.getUTCDate();
          const originalUTCHours = actualEndDate.getUTCHours();
          let hours = originalUTCHours - offsetHours;
          const minutes = actualEndDate.getUTCMinutes();
          const seconds = actualEndDate.getUTCSeconds();
          
          // Handle hour underflow (if subtracting offset makes hours negative, go to previous day)
          if (hours < 0) {
            hours += 24;
            // Use Date constructor which handles month/day boundaries correctly
            actualEndDate = new Date(year, month, day - 1, hours, minutes, seconds);
          } else {
            actualEndDate = new Date(year, month, day, hours, minutes, seconds);
          }
        }
        
        // Validate that dates are valid
        if (isNaN(actualStartDate.getTime()) || isNaN(actualEndDate.getTime())) {
          console.warn(`[parseUnifiedDateTime] Invalid date after conversion. Original: ${cleanStr}`);
          return null;
        }
        
        // Handle cross-midnight events: if start > end after Pacific conversion,
        // this means the event spans midnight and the start should be on the PREVIOUS day
        // (NOT a swap - move start back 24 hours instead)
        if (actualStartDate.getTime() > actualEndDate.getTime()) {
          // Move start back by 24 hours (one day earlier) - this is a cross-midnight event
          actualStartDate = new Date(actualStartDate.getTime() - 24 * 60 * 60 * 1000);
        }
        
        return {
          start: actualStartDate,
          end: actualEndDate
        };
      }
    } catch (e) {
      console.warn('Failed to parse date range format:', cleanStr, e);
    }
  }
  
  // Fallback: try to parse as regular ISO date
  try {
    const date = new Date(cleanStr);
    
    if (!isNaN(date.getTime())) {
      // For UTC times (ISO timestamps with Z or +00:00), add Pacific offset to convert to Pacific floating time
      const isUTCTime = cleanStr.includes('T') && (cleanStr.includes('Z') || cleanStr.includes('+00:00'));
      
      if (isUTCTime) {
        // Check if this appears to be an all-day event (midnight UTC time)
        const isAllDay = cleanStr.match(/T00:00:00/);
        
        if (isAllDay) {
          // For all-day events, extract date components and create date at midnight Pacific
          // This prevents date shifting when converting from UTC
          const year = date.getUTCFullYear();
          const month = date.getUTCMonth();
          const day = date.getUTCDate();
          
          // Create new date at midnight Pacific time (floating, no timezone)
          const pacificDate = new Date(year, month, day, 0, 0, 0);
          
          return {
            start: pacificDate,
            end: pacificDate
          };
        }
        
        // For timed events, subtract Pacific offset to convert UTC to Pacific floating time
        // UTC is ahead of Pacific, so we subtract hours
        const isDST = isDSTDate(date);
        const offsetHours = isDST ? 7 : 8;
        date.setHours(date.getHours() - offsetHours);
      }
      
      return {
        start: date,
        end: date
      };
    }
  } catch (e) {
    console.warn('Failed to parse as ISO date:', cleanStr, e);
  }
  
  return null;
}

// Helper function to regenerate calendar for a single person
async function regenerateCalendarForPerson(personId) {
  try {
    console.log(`🔄 Regenerating calendar for ${personId}...`);
    
    // CLEAR CACHE FIRST to ensure fresh data from Notion
    if (redis && cacheEnabled) {
      const icsKey = `calendar:${personId}:ics`;
      const jsonKey = `calendar:${personId}:json`;
      
      // Verify cache exists before deletion (for logging)
      const icsExists = await redis.exists(icsKey);
      const jsonExists = await redis.exists(jsonKey);
      
      // Delete both cache entries
      const icsDeleted = await redis.del(icsKey);
      const jsonDeleted = await redis.del(jsonKey);
      
      // Verify cache is actually gone
      const icsStillExists = await redis.exists(icsKey);
      const jsonStillExists = await redis.exists(jsonKey);
      
      console.log(`🗑️  Cache clearing for ${personId}:`);
      console.log(`   ICS cache - Before: ${icsExists ? 'EXISTS' : 'MISS'}, Deleted: ${icsDeleted}, After: ${icsStillExists ? 'STILL EXISTS ❌' : 'CLEARED ✅'}`);
      console.log(`   JSON cache - Before: ${jsonExists ? 'EXISTS' : 'MISS'}, Deleted: ${jsonDeleted}, After: ${jsonStillExists ? 'STILL EXISTS ❌' : 'CLEARED ✅'}`);
      
      if (icsStillExists || jsonStillExists) {
        console.error(`⚠️  WARNING: Cache was not fully cleared for ${personId}!`);
      }
    }
    
    // Get calendar data from Calendar Data database (fresh from Notion API)
    const calendarData = await getCalendarDataFromDatabase(personId);
    if (!calendarData || !calendarData.events || calendarData.events.length === 0) {
      console.log(`⚠️  No events found for ${personId}, skipping...`);
      return { success: false, personId, reason: 'no_events' };
    }
    
    const events = calendarData;
    
    // Get person name from Personnel database
    const person = await retryNotionCall(() => 
      notion.pages.retrieve({ page_id: personId })
    );
    const personName = person.properties?.['Full Name']?.formula?.string || 'Unknown';
    const firstName = person.properties?.['First Name']?.formula?.string || personName.split(' ')[0];
    
    console.log(`Processing calendar for ${personName} (${calendarData.events.length} events)`);

    // Process events into calendar format (duplicated from main endpoint)
    const allCalendarEvents = [];
    
    const eventsArray = Array.isArray(events) ? events : events.events || [];
    const topLevelFlights = events.flights || [];
    const topLevelRehearsals = events.rehearsals || [];
    const topLevelHotels = events.hotels || [];
    const topLevelTransport = events.ground_transport || [];
    const topLevelTeamCalendar = events.team_calendar || [];
    
    eventsArray.forEach(event => {
      // Add main event
      if (event.event_name && event.event_date) {
        // Debug logging for events crossing midnight
        if (event.event_name.includes('Gold Standard') || event.event_name.includes('Wedding')) {
          console.log(`[DEBUG] Event: ${event.event_name}`);
          console.log(`[DEBUG] event_date: ${event.event_date}`);
        }
        let eventTimes = parseUnifiedDateTime(event.event_date);
        
        if (eventTimes) {
          // Debug logging for parsed times
          if (event.event_name && (event.event_name.includes('Gold Standard') || event.event_name.includes('Wedding'))) {
            console.log(`[DEBUG] Parsed start: ${eventTimes.start}`);
            console.log(`[DEBUG] Parsed end: ${eventTimes.end}`);
            console.log(`[DEBUG] Start > End? ${eventTimes.start.getTime() > eventTimes.end.getTime()}`);
          }
          let payrollInfo = '';
          if (event.position || event.pay_total || event.assignments) {
            if (event.position) payrollInfo += `Position: ${event.position}\n`;
            if (event.assignments) payrollInfo += `Assignments: ${event.assignments}\n`;
            if (event.pay_total) payrollInfo += `Pay: $${event.pay_total}\n`;
            payrollInfo += '\n';
          }

          let calltimeInfo = '';
          if (event.calltime) {
            calltimeInfo = `➡️ Call Time: ${formatCallTime(event.calltime)}\n\n`;
          }

          let gearChecklistInfo = '';
          if (event.gear_checklist && event.gear_checklist.trim()) {
            gearChecklistInfo = `🔧 Gear Checklist: ${event.gear_checklist}\n\n`;
          }

          // Build event personnel info (after gear checklist, before general info)
          let eventPersonnelInfo = '';
          if (event.event_personnel && event.event_personnel.trim()) {
            eventPersonnelInfo = `👥 Event Personnel:\n${event.event_personnel}\n\n`;
          }

          let notionUrlInfo = '';
          if (event.notion_url && event.notion_url.trim()) {
            notionUrlInfo = `Notion Link: ${event.notion_url}\n\n`;
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + calltimeInfo + gearChecklistInfo + eventPersonnelInfo + notionUrlInfo + (event.general_info || ''),
            location: event.venue_address || event.venue || '',
            band: event.band || '',
            mainEvent: event.event_name
          });
        }
      }
      
      // Add flight events (same logic)
      if (event.flights && Array.isArray(event.flights)) {
        event.flights.forEach(flight => {
          if (flight.departure_time && flight.departure_name) {
            let departureTimes = parseUnifiedDateTime(flight.departure_time);
            if (!departureTimes) {
              departureTimes = {
                start: flight.departure_time,
                end: flight.departure_arrival_time || flight.departure_time
              };
            }
            
            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          if (flight.return_time && flight.return_name) {
            let returnTimes = parseUnifiedDateTime(flight.return_time);
            if (!returnTimes) {
              returnTimes = {
                start: flight.return_time,
                end: flight.return_arrival_time || flight.return_time
              };
            }
            
            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Departure layover flight
          if (flight.departure_lo_time && flight.departure_lo_flightnumber) {
            let departureLoTimes = parseUnifiedDateTime(flight.departure_lo_time);
            if (!departureLoTimes) {
              departureLoTimes = {
                start: flight.departure_lo_time,
                end: flight.departure_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_departure_layover',
              title: `✈️ Layover: ${flight.departure_lo_from_airport || 'N/A'} → ${flight.departure_lo_to_airport || 'N/A'}`,
              start: departureLoTimes.start,
              end: departureLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_lo_flightnumber || 'N/A'}\nFrom: ${flight.departure_lo_from_airport || 'N/A'}\nTo: ${flight.departure_lo_to_airport || 'N/A'}`,
              location: flight.departure_lo_from_airport_address || flight.departure_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Return layover flight
          if (flight.return_lo_time && flight.return_lo_flightnumber) {
            let returnLoTimes = parseUnifiedDateTime(flight.return_lo_time);
            if (!returnLoTimes) {
              returnLoTimes = {
                start: flight.return_lo_time,
                end: flight.return_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_return_layover',
              title: `✈️ Layover: ${flight.return_lo_from_airport || 'N/A'} → ${flight.return_lo_to_airport || 'N/A'}`,
              start: returnLoTimes.start,
              end: returnLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_lo_flightnumber || 'N/A'}\nFrom: ${flight.return_lo_from_airport || 'N/A'}\nTo: ${flight.return_lo_to_airport || 'N/A'}`,
              location: flight.return_lo_from_airport_address || flight.return_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add rehearsal events (same logic)
      if (event.rehearsals && Array.isArray(event.rehearsals)) {
        event.rehearsals.forEach(rehearsal => {
          if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
            let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
            let location = 'TBD';
            if (rehearsal.rehearsal_location && rehearsal.rehearsal_address) {
              location = `${rehearsal.rehearsal_location}, ${rehearsal.rehearsal_address}`;
            } else if (rehearsal.rehearsal_location) {
              location = rehearsal.rehearsal_location;
            } else if (rehearsal.rehearsal_address) {
              location = rehearsal.rehearsal_address;
            }

            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal - ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add hotel events (same logic)
      if (event.hotels && Array.isArray(event.hotels)) {
        event.hotels.forEach(hotel => {
          let hotelTimes = null;
          
          if (hotel.dates_booked) {
            hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
          } else if (hotel.check_in && hotel.check_out) {
            try {
              hotelTimes = parseUnifiedDateTime(hotel.check_in);
              if (!hotelTimes) {
                const startDate = new Date(hotel.check_in);
                const endDate = new Date(hotel.check_out);
                const isDST = isDSTDate(startDate);
                const offsetHours = isDST ? 7 : 8;
                startDate.setHours(startDate.getHours() + offsetHours);
                endDate.setHours(endDate.getHours() + offsetHours);
                hotelTimes = { start: startDate, end: endDate };
              }
            } catch (e) {
              console.warn('Unable to parse hotel dates:', hotel.check_in, hotel.check_out);
              return;
            }
          }

          if (hotelTimes) {
            let namesFormatted = 'N/A';
            if (hotel.names_on_reservation) {
              const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
              if (names.length > 0) {
                namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
              }
            }

            allCalendarEvents.push({
              type: 'hotel',
              title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
              start: hotelTimes.start,
              end: hotelTimes.end,
              description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
              location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
              url: hotel.hotel_url || '',
              confirmation: hotel.confirmation || '',
              hotelName: hotel.hotel_name || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add ground transport events (same logic)
      if (event.ground_transport && Array.isArray(event.ground_transport)) {
        event.ground_transport.forEach(transport => {
          if (transport.start && transport.end) {
            let transportTimes = parseUnifiedDateTime(transport.start);
            if (!transportTimes) {
              transportTimes = {
                start: transport.start,
                end: transport.end || transport.start
              };
            }

            const startTime = new Date(transportTimes.start);
            const endTime = new Date(startTime.getTime() + 30 * 60 * 1000);

            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');

            let description = '';
            if (transport.description) {
              const driverMatch = transport.description.match(/Driver:\s*([^\n]+)/);
              const passengerMatch = transport.description.match(/Passenger:\s*([^\n]+)/);
              
              if (driverMatch) {
                const drivers = driverMatch[1].split(',').map(d => d.trim()).filter(d => d);
                if (drivers.length > 0) {
                  description += 'Drivers:\n';
                  drivers.forEach(driver => {
                    description += `• ${driver}\n`;
                  });
                  description += '\n';
                }
              }
              
              if (passengerMatch && transport.type === 'ground_transport_meeting') {
                const passengers = passengerMatch[1].split(',').map(p => p.trim()).filter(p => p);
                if (passengers.length > 0) {
                  description += 'Passengers:\n';
                  passengers.forEach(passenger => {
                    description += `• ${passenger}\n`;
                  });
                  description += '\n';
                }
              }
              
              if (transport.type === 'ground_transport_meeting') {
                const meetupInfoMatch = transport.description.match(/Meet Up Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (meetupInfoMatch) {
                  const meetupInfo = meetupInfoMatch[1].trim();
                  if (meetupInfo) {
                    description += 'Meet Up Info:\n';
                    const meetupInfoLines = meetupInfo.split('\n').filter(line => line.trim());
                    meetupInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) description += `• ${trimmedLine}\n`;
                    });
                    description += '\n';
                  }
                }
              } else if (transport.type === 'ground_transport_pickup') {
                const pickupInfoMatch = transport.description.match(/Pick Up Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (pickupInfoMatch) {
                  const pickupInfo = pickupInfoMatch[1].trim();
                  if (pickupInfo) {
                    description += 'Pick Up Info:\n';
                    const pickupInfoLines = pickupInfo.split('\n').filter(line => line.trim());
                    pickupInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) description += `• ${trimmedLine}\n`;
                    });
                    description += '\n';
                  }
                }
              } else if (transport.type === 'ground_transport_dropoff') {
                const dropoffInfoMatch = transport.description.match(/Drop Off Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (dropoffInfoMatch) {
                  const dropoffInfo = dropoffInfoMatch[1].trim();
                  if (dropoffInfo) {
                    description += 'Drop Off Info:\n';
                    const dropoffInfoLines = dropoffInfo.split('\n').filter(line => line.trim());
                    dropoffInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) description += `• ${trimmedLine}\n`;
                    });
                    description += '\n';
                  }
                }
              }
              
              const confirmationMatch = transport.description.match(/Confirmation:\s*([^\n]+)/);
              if (confirmationMatch) {
                description += `Confirmation: ${confirmationMatch[1]}\n`;
              }
              
              if (transport.transportation_url) {
                description += `\nNotion Link: ${transport.transportation_url}`;
              }
            } else {
              description = 'Ground transportation details';
            }

            allCalendarEvents.push({
              type: transport.type || 'ground_transport',
              title: `🚙 ${formattedTitle}`,
              start: startTime.toISOString(),
              end: endTime.toISOString(),
              description: description.trim(),
              location: transport.location || '',
              mainEvent: event.event_name
            });
          }
        });
      }
    });
    
    // Process top-level arrays (same logic as main endpoint)
    if (topLevelFlights.length > 0) {
      topLevelFlights.forEach(flight => {
        if (flight.departure_time && flight.departure_name) {
          let departureTimes = parseUnifiedDateTime(flight.departure_time);
          if (departureTimes) {
            let description = `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: description,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }
        }

        if (flight.return_time && flight.return_name) {
          let returnTimes = parseUnifiedDateTime(flight.return_time);
          if (returnTimes) {
            let description = `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: description,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }

          // Departure layover flight
          if (flight.departure_lo_time && flight.departure_lo_flightnumber) {
            let departureLoTimes = parseUnifiedDateTime(flight.departure_lo_time);
            if (!departureLoTimes) {
              departureLoTimes = {
                start: flight.departure_lo_time,
                end: flight.departure_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_departure_layover',
              title: `✈️ Layover: ${flight.departure_lo_from_airport || 'N/A'} → ${flight.departure_lo_to_airport || 'N/A'}`,
              start: departureLoTimes.start,
              end: departureLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_lo_flightnumber || 'N/A'}\nFrom: ${flight.departure_lo_from_airport || 'N/A'}\nTo: ${flight.departure_lo_to_airport || 'N/A'}`,
              location: flight.departure_lo_from_airport_address || flight.departure_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }

          // Return layover flight
          if (flight.return_lo_time && flight.return_lo_flightnumber) {
            let returnLoTimes = parseUnifiedDateTime(flight.return_lo_time);
            if (!returnLoTimes) {
              returnLoTimes = {
                start: flight.return_lo_time,
                end: flight.return_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_return_layover',
              title: `✈️ Layover: ${flight.return_lo_from_airport || 'N/A'} → ${flight.return_lo_to_airport || 'N/A'}`,
              start: returnLoTimes.start,
              end: returnLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_lo_flightnumber || 'N/A'}\nFrom: ${flight.return_lo_from_airport || 'N/A'}\nTo: ${flight.return_lo_to_airport || 'N/A'}`,
              location: flight.return_lo_from_airport_address || flight.return_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelRehearsals.length > 0) {
      topLevelRehearsals.forEach(rehearsal => {
        if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
          let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
          
          if (rehearsalTimes) {
            let location = rehearsal.rehearsal_address ? rehearsal.rehearsal_address.trim().replace(/\u2060/g, '') : 'TBD';
            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelHotels.length > 0) {
      topLevelHotels.forEach(hotel => {
        let hotelTimes = null;
        if (hotel.dates_booked) {
          hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
        }

        if (hotelTimes) {
          let namesFormatted = 'N/A';
          if (hotel.names_on_reservation) {
            const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
            if (names.length > 0) {
              namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
            }
          }

          allCalendarEvents.push({
            type: 'hotel',
            title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
            start: hotelTimes.start,
            end: hotelTimes.end,
            description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
            location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
            url: hotel.hotel_url || '',
            confirmation: hotel.confirmation || '',
            hotelName: hotel.hotel_name || '',
            mainEvent: ''
          });
        }
      });
    }

    if (topLevelTransport.length > 0) {
      topLevelTransport.forEach(transport => {
        if (transport.start) {
          let transportTimes = parseUnifiedDateTime(transport.start);
          if (transportTimes) {
            const startTime = new Date(transportTimes.start);
            const endTime = new Date(startTime.getTime() + 30 * 60 * 1000);

            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');

            let description = '';
            if (transport.description) {
              const driverMatch = transport.description.match(/Driver:\s*([^\n]+)/);
              if (driverMatch) {
                const drivers = driverMatch[1].split(',').map(d => d.trim()).filter(d => d);
                if (drivers.length > 0) {
                  description += 'Drivers:\n';
                  drivers.forEach(driver => {
                    description += `• ${driver}\n`;
                  });
                  description += '\n';
                }
              }
              description += transport.description.replace(/Driver:\s*[^\n]+\n?/, '');
            }
            
            if (transport.transportation_url) {
              description += `\n\nNotion Link: ${transport.transportation_url}`;
            }

            let eventType = 'ground_transport';
            if (transport.type === 'ground_transport_pickup') {
              eventType = 'ground_transport_pickup';
            } else if (transport.type === 'ground_transport_dropoff') {
              eventType = 'ground_transport_dropoff';
            } else if (transport.type === 'ground_transport_meeting') {
              eventType = 'ground_transport_meeting';
            }

            allCalendarEvents.push({
              type: eventType,
              title: `🚙 ${formattedTitle}`,
              start: startTime,
              end: endTime,
              description: description,
              location: transport.location || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelTeamCalendar.length > 0) {
      topLevelTeamCalendar.forEach(teamEvent => {
        if (teamEvent.date) {
          let eventTimes = parseUnifiedDateTime(teamEvent.date);
          if (eventTimes) {
            const isOOO = teamEvent.title && teamEvent.title.trim().toUpperCase() === 'OOO';
            const isMeeting = teamEvent.title && teamEvent.title.trim().toUpperCase().includes('MEETING');
            let emoji;
            if (isOOO) {
              emoji = '⛔️';
            } else if (isMeeting) {
              emoji = '💼';
            } else {
              emoji = '📅';
            }
            
            // For OOO events, add one day to end date to make it inclusive
            // In iCal format, end date is exclusive, so we need Dec 17 to block through Dec 16
            let endDate = eventTimes.end;
            if (isOOO) {
              endDate = new Date(eventTimes.end);
              endDate.setDate(endDate.getDate() + 1);
            }
            
            allCalendarEvents.push({
              type: 'team_calendar',
              title: `${emoji} ${teamEvent.title || 'Team Event'}`,
              start: eventTimes.start,
              end: endDate,
              description: [teamEvent.dcos, teamEvent.notes].filter(Boolean).join('\n\n'),
              location: teamEvent.address || '',
              url: teamEvent.notion_link || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    // Generate ICS calendar
    const calendar = ical({ 
      name: `Downbeat iCal (${firstName})`,
      description: `Professional events calendar for ${personName}`,
      ttl: 300  // Suggest refresh every 5 minutes
    });

    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });

    const icsData = calendar.toString();
    
    // Cache the ICS data
    if (redis && cacheEnabled) {
      const cacheKey = `calendar:${personId}:ics`;
      await redis.setEx(cacheKey, CACHE_TTL, icsData);
      console.log(`✅ Cached ICS for ${personName} (${allCalendarEvents.length} events, TTL: ${CACHE_TTL}s)`);
    }

    return { success: true, personId, personName, eventCount: allCalendarEvents.length };
    
  } catch (error) {
    console.error(`❌ Error regenerating calendar for ${personId}:`, error.message);
    return { success: false, personId, error: error.message };
  }
}

// Helper function to regenerate all calendars using batched parallel processing
async function regenerateAllCalendars() {
  const startTime = Date.now();
  
  try {
    console.log('🚀 Starting BATCHED PARALLEL calendar regeneration...');
    
    // Get all person IDs from Personnel database
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      page_size: 100
    });

    const personIds = response.results.map(page => page.id);
    console.log(`Found ${personIds.length} people in Personnel database`);
    console.log(`Processing ${personIds.length} people in batches of 100...`);
    
    const batchSize = 100;
    const batches = [];
    for (let i = 0; i < personIds.length; i += batchSize) {
      batches.push(personIds.slice(i, i + batchSize));
    }
    
    let totalSuccess = 0;
    let totalFailed = 0;
    let totalSkipped = 0;
    const allResults = [];
    
    // Process each batch
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      const batchStart = Date.now();
      
      console.log(`\n📦 Processing batch ${batchIndex + 1}/${batches.length} (${batch.length} people)...`);
      
      // Process batch in parallel
      const batchPromises = batch.map(async (personId) => {
        try {
          return await regenerateCalendarForPerson(personId);
        } catch (error) {
          console.error(`❌ Failed to process ${personId}:`, error.message);
          return { success: false, personId, error: error.message };
        }
      });
      
      // Wait for batch to complete
      const batchResults = await Promise.all(batchPromises);
      
      // Count batch results
      const batchSuccess = batchResults.filter(r => r.success).length;
      const batchSkipped = batchResults.filter(r => r.reason === 'no_events').length;
      const batchFailed = batchResults.filter(r => !r.success && r.reason !== 'no_events').length;
      
      totalSuccess += batchSuccess;
      totalSkipped += batchSkipped;
      totalFailed += batchFailed;
      allResults.push(...batchResults);
      
      const batchTime = Math.round((Date.now() - batchStart) / 1000);
      console.log(`   ✅ Batch ${batchIndex + 1} complete in ${batchTime}s: ${batchSuccess} success, ${batchFailed} failed, ${batchSkipped} skipped`);
      
      // Add delay between batches to avoid overwhelming Notion API
      if (batchIndex < batches.length - 1) {
        console.log('   ⏳ Waiting 5s before next batch...');
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
    
    const totalTime = Math.round((Date.now() - startTime) / 1000);
    
    console.log(`\n✅ Batched parallel regeneration complete in ${totalTime}s!`);
    console.log(`   Total: ${personIds.length} people, ${batches.length} batches`);
    console.log(`   Success: ${totalSuccess}, Failed: ${totalFailed}, Skipped: ${totalSkipped}`);
    
    return { 
      success: true, 
      total: personIds.length, 
      batches: batches.length,
      successCount: totalSuccess, 
      failCount: totalFailed, 
      skippedCount: totalSkipped, 
      results: allResults,
      timeSeconds: totalTime
    };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`❌ Error in batched regeneration after ${elapsed}s:`, error);
    return { success: false, error: error.message, timeSeconds: elapsed };
  }
}

// Background job to update all people in parallel batches every 5 minutes
function startBackgroundJob() {
  console.log('🔄 Starting background calendar refresh job (every 5 minutes)');
  console.log('   Processing all people in parallel batches each cycle');
  
  setInterval(async () => {
    try {
      const jobStart = Date.now();
      console.log('\n⏰ Background job triggered - fetching all people...');
      
      // Get all person IDs from Personnel database with pagination
      const personIds = [];
      let cursor = undefined;
      let hasMore = true;
      
      while (hasMore) {
        const queryParams = {
          database_id: PERSONNEL_DB,
          page_size: 100
        };
        
        if (cursor) {
          queryParams.start_cursor = cursor;
        }
        
        const response = await notion.databases.query(queryParams);
        
        // Add person IDs from this page
        const pagePersonIds = response.results.map(page => page.id);
        personIds.push(...pagePersonIds);
        
        // Check if there are more pages
        hasMore = response.has_more;
        cursor = response.next_cursor;
        
        console.log(`   Fetched ${pagePersonIds.length} people (total so far: ${personIds.length})`);
      }
      
      if (personIds.length === 0) {
        console.log('⚠️  No personnel found in database');
        return;
      }
      
      console.log(`   Found ${personIds.length} total people to update`);
      
      // Process all people in parallel (batches of 100)
      const batchSize = 100;
      const batches = [];
      for (let i = 0; i < personIds.length; i += batchSize) {
        batches.push(personIds.slice(i, i + batchSize));
      }
      
      let totalSuccess = 0;
      let totalFailed = 0;
      let totalSkipped = 0;
      
      // Process each batch
      for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        const batch = batches[batchIndex];
        
        // Process batch in parallel
        const batchPromises = batch.map(async (personId) => {
          try {
            return await regenerateCalendarForPerson(personId);
          } catch (error) {
            return { success: false, personId, error: error.message };
          }
        });
        
        // Wait for batch to complete
        const batchResults = await Promise.all(batchPromises);
        
        // Count results
        const batchSuccess = batchResults.filter(r => r.success).length;
        const batchSkipped = batchResults.filter(r => r.reason === 'no_events').length;
        const batchFailed = batchResults.filter(r => !r.success && r.reason !== 'no_events').length;
        
        totalSuccess += batchSuccess;
        totalSkipped += batchSkipped;
        totalFailed += batchFailed;
      }
      
      // Also refresh admin calendar
      if (ADMIN_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        try {
          console.log('🔄 Refreshing admin calendar...');
          const adminEvents = await getAdminCalendarData();
          
          if (adminEvents && adminEvents.length > 0) {
            const allCalendarEvents = processAdminEvents(adminEvents);
            
            // Generate and cache ICS
            const calendar = ical({ 
              name: 'Admin Calendar',
              description: 'All upcoming events',
              ttl: 300
            });
            
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: getAlarmsForEvent(event.type, event.title)
              });
            });
            
            const icsData = calendar.toString();
            await redis.setEx('calendar:admin:ics', CACHE_TTL, icsData);
            
            // Also cache JSON
            const jsonData = JSON.stringify({
              calendar_name: 'Admin Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await redis.setEx('calendar:admin:json', CACHE_TTL, jsonData);
            
            console.log(`✅ Admin calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (adminError) {
          console.error('⚠️  Admin calendar refresh failed:', adminError.message);
        }
      }
      
      // Also refresh travel calendar
      if (TRAVEL_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        try {
          console.log('🔄 Refreshing travel calendar...');
          const travelEvents = await getTravelCalendarData();
          
          if (travelEvents && travelEvents.length > 0) {
            const allCalendarEvents = processTravelEvents(travelEvents);
            
            // Generate and cache ICS
            const calendar = ical({ 
              name: 'Travel Calendar',
              description: 'All travel events',
              ttl: 300
            });
            
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: getAlarmsForEvent(event.type, event.title)
              });
            });
            
            const icsData = calendar.toString();
            await redis.setEx('calendar:travel:ics', CACHE_TTL, icsData);
            
            // Also cache JSON
            const jsonData = JSON.stringify({
              calendar_name: 'Travel Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await redis.setEx('calendar:travel:json', CACHE_TTL, jsonData);
            
            console.log(`✅ Travel calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (travelError) {
          console.error('⚠️  Travel calendar refresh failed:', travelError.message);
        }
      }
      
      // Also refresh blockout calendar
      if (BLOCKOUT_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        try {
          console.log('🔄 Refreshing blockout calendar...');
          const blockoutEvents = await getBlockoutCalendarData();
          if (blockoutEvents && blockoutEvents.length > 0) {
            const allCalendarEvents = processBlockoutEvents(blockoutEvents);
            const calendar = ical({ 
              name: 'Blockout Calendar',
              description: 'All blockout events',
              ttl: 300
            });
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: getAlarmsForEvent(event.type, event.title)
              });
            });
            const icsData = calendar.toString();
            await redis.setEx('calendar:blockout:ics', CACHE_TTL, icsData);
            const jsonData = JSON.stringify({
              calendar_name: 'Blockout Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await redis.setEx('calendar:blockout:json', CACHE_TTL, jsonData);
            console.log(`✅ Blockout calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (blockoutError) {
          console.error('⚠️  Blockout calendar refresh failed:', blockoutError.message);
        }
      }
      
      const jobTime = Math.round((Date.now() - jobStart) / 1000);
      console.log(`✅ Background refresh complete in ${jobTime}s: ${totalSuccess} success, ${totalFailed} failed, ${totalSkipped} skipped (processed ${personIds.length} total people + admin calendar + travel calendar + blockout calendar)`);
      
    } catch (error) {
      console.error('❌ Background job error:', error.message);
    }
  }, 5 * 60 * 1000); // 5 minutes
}

// ============================================
// ADMIN CALENDAR FUNCTIONS
// ============================================

// Helper function to get admin calendar data by page ID
async function getAdminCalendarData() {
  if (!ADMIN_CALENDAR_PAGE_ID) {
    throw new Error('ADMIN_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = ADMIN_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Admin Events property
  // Note: Using full page retrieve for now - property-specific retrieve may need property ID (UUID)
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Admin Events property
  const adminEventsString = page.properties['Admin Events']?.formula?.string || 
                            page.properties['Admin Events']?.rich_text?.[0]?.text?.content ||
                            '[]';

  try {
    const adminEvents = JSON.parse(adminEventsString);
    return Array.isArray(adminEvents) ? adminEvents : [];
  } catch (e) {
    console.error('Error parsing Admin Events JSON:', adminEventsString?.substring(0, 100));
    throw new Error(`Admin Events JSON parse error: ${e.message}`);
  }
}

// Helper function to process admin events into calendar format
function processAdminEvents(eventsArray) {
  const allCalendarEvents = [];

  eventsArray.forEach(event => {
    // Process main events (same logic as existing main_event processing)
    if (event.event_name && event.event_date) {
      let eventTimes = parseUnifiedDateTime(event.event_date);
      
      if (eventTimes) {
        // Build payroll info for description (put at TOP)
        let payrollInfo = '';
        
        if (event.position || event.pay_total || event.assignments) {
          if (event.position) {
            payrollInfo += `Position: ${event.position}\n`;
          }
          if (event.pay_total) {
            payrollInfo += `Pay: ${event.pay_total}\n`;
          }
          if (event.assignments) {
            payrollInfo += `Assignments: ${event.assignments}\n`;
          }
          payrollInfo += '\n---\n\n';
        }

        // Build description sections
        let description = payrollInfo; // Payroll info at top
        
        // Calltime
        if (event.calltime) {
          description += `🕐 Calltime: ${event.calltime}\n`;
        }
        
        // Gear
        if (event.gear) {
          description += `🎸 Gear: ${event.gear}\n`;
        }
        
        // Personnel (handle both string and array formats)
        if (event.event_personnel) {
          if (typeof event.event_personnel === 'string') {
            // Split by newlines if it's a string
            description += `\n👥 Personnel:\n${event.event_personnel}\n`;
          } else if (Array.isArray(event.event_personnel) && event.event_personnel.length > 0) {
            description += `\n👥 Personnel:\n`;
            event.event_personnel.forEach(person => {
              description += `  • ${person}\n`;
            });
          }
        }
        
        // General Info / Notes
        if (event.general_info) {
          description += `\n📋 General Info:\n${event.general_info}\n`;
        }
        
        // Notion URL is in URL field, not in description

        let title = event.event_name;
        
        // Add guitar emoji for gigs
        if (title && (
          title.toLowerCase().includes('gig') ||
          title.toLowerCase().includes('show') ||
          title.toLowerCase().includes('performance') ||
          title.toLowerCase().includes('concert')
        )) {
          title = `🎸 ${title}`;
        }

        // Build location from venue and venue_address
        let location = '';
        if (event.venue && event.venue_address) {
          location = `${event.venue}, ${event.venue_address}`;
        } else if (event.venue_address) {
          location = event.venue_address;
        } else if (event.venue) {
          location = event.venue;
        }

        allCalendarEvents.push({
          start: eventTimes.start,
          end: eventTimes.end,
          title: title,
          description: description.trim(),
          location: location,
          url: event.notion_url || '',
          type: 'main_event'
        });
      }
    }

    // Process rehearsals for this event
    if (event.rehearsals && Array.isArray(event.rehearsals)) {
      event.rehearsals.forEach(rehearsal => {
        if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
          let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
          
          if (rehearsalTimes) {
            // Build location from rehearsal_location and rehearsal_address
            let location = 'TBD';
            if (rehearsal.rehearsal_location && rehearsal.rehearsal_address) {
              location = `${rehearsal.rehearsal_location}, ${rehearsal.rehearsal_address}`;
            } else if (rehearsal.rehearsal_location) {
              location = rehearsal.rehearsal_location;
            } else if (rehearsal.rehearsal_address) {
              location = rehearsal.rehearsal_address.trim().replace(/\u2060/g, '');
            }

            // Build description
            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            // Build title with event name and band if available
            let title = `🎤 Rehearsal - ${event.event_name || 'Event'}`;
            if (event.band) {
              title += ` (${event.band})`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: title,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_pco || '',
              mainEvent: event.event_name || ''
            });
          }
        }
      });
    }
  });

  return allCalendarEvents;
}

// ============================================
// TRAVEL CALENDAR FUNCTIONS
// ============================================

// Helper function to get travel calendar data by page ID
async function getTravelCalendarData() {
  if (!TRAVEL_CALENDAR_PAGE_ID) {
    throw new Error('TRAVEL_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = TRAVEL_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Travel Admin property
  // Note: Using full page retrieve for now - property-specific retrieve may need property ID (UUID)
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Travel Admin property
  let travelEventsString = page.properties['Travel Admin']?.formula?.string || 
                          page.properties['Travel Admin']?.rich_text?.[0]?.text?.content ||
                          '[]';

  // Clean the string - remove any leading/trailing whitespace
  travelEventsString = travelEventsString.trim();

  // Try to extract JSON if there's extra text (look for first [ and last ])
  if (travelEventsString.includes('[') && travelEventsString.includes(']')) {
    const firstBracket = travelEventsString.indexOf('[');
    const lastBracket = travelEventsString.lastIndexOf(']');
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      travelEventsString = travelEventsString.substring(firstBracket, lastBracket + 1);
    }
  }

  // Fix double commas (common JSON formatting issue)
  travelEventsString = travelEventsString.replace(/,,+/g, ',');
  
  // Fix malformed arrays where "personnel_ids" appears as a key inside the personnel array
  // Pattern: ["Name1","Name2","personnel_ids":[...] should become ["Name1","Name2"]
  // This handles cases where personnel_ids key appears inside the personnel array
  travelEventsString = travelEventsString.replace(/"personnel":\[([^\]]*)"personnel_ids":/g, (match, personnelList) => {
    // Remove any trailing commas from personnel list
    const cleaned = personnelList.replace(/,\s*$/, '');
    return `"personnel":[${cleaned}]`;
  });

  try {
    const travelEvents = JSON.parse(travelEventsString);
    return Array.isArray(travelEvents) ? travelEvents : [];
  } catch (e) {
    console.error('Error parsing Travel Admin JSON. First 200 chars:', travelEventsString?.substring(0, 200));
    console.error('Full length:', travelEventsString?.length);
    console.error('Parse error:', e.message);
    // Try to find the problematic area
    const errorPos = parseInt(e.message.match(/position (\d+)/)?.[1]) || 0;
    if (errorPos > 0) {
      const start = Math.max(0, errorPos - 100);
      const end = Math.min(travelEventsString.length, errorPos + 100);
      console.error('Problem area:', travelEventsString.substring(start, end));
    }
    throw new Error(`Travel Admin JSON parse error: ${e.message}. First 200 chars: ${travelEventsString?.substring(0, 200)}`);
  }
}

// Debug collector for travel calendar
let travelDebugLogs = [];
function travelDebugLog(message) {
  console.log(message);
  travelDebugLogs.push(message);
}
function clearTravelDebugLogs() { travelDebugLogs = []; }
function getTravelDebugLogs() { return travelDebugLogs; }

// Helper function to process travel events into calendar format
function processTravelEvents(travelGroupsArray) {
  const allCalendarEvents = [];
  clearTravelDebugLogs();

  // Each element is a travel group with flights, hotels, and ground_transportation
  travelGroupsArray.forEach(travelGroup => {
    // Process flights
    if (travelGroup.flights && Array.isArray(travelGroup.flights)) {
      travelGroup.flights.forEach(flight => {
        // Departure flight
        if (flight.departure_time && flight.departure_arrival_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const depTimes = parseUnifiedDateTime(flight.departure_time);
          const depEndTimes = parseUnifiedDateTime(flight.departure_arrival_time);
          const depStart = depTimes ? depTimes.start : new Date(flight.departure_time);
          const depEnd = depEndTimes ? depEndTimes.start : new Date(flight.departure_arrival_time);
          
          // DEBUG: Log flight time conversion
          if (flight.departure_name && flight.departure_name.includes('RSW')) {
            travelDebugLog(`[DEBUG-TRAVEL] Flight: ${flight.departure_name}`);
            travelDebugLog(`[DEBUG-TRAVEL] Raw departure_time: ${flight.departure_time}`);
            travelDebugLog(`[DEBUG-TRAVEL] Raw departure_arrival_time: ${flight.departure_arrival_time}`);
            travelDebugLog(`[DEBUG-TRAVEL] depTimes from parseUnifiedDateTime: ${depTimes ? JSON.stringify({start: depTimes.start.toString(), end: depTimes.end.toString()}) : 'null'}`);
            travelDebugLog(`[DEBUG-TRAVEL] depEndTimes from parseUnifiedDateTime: ${depEndTimes ? JSON.stringify({start: depEndTimes.start.toString()}) : 'null'}`);
            travelDebugLog(`[DEBUG-TRAVEL] Final depStart: ${depStart.toString()}`);
            travelDebugLog(`[DEBUG-TRAVEL] Final depEnd: ${depEnd.toString()}`);
          }
          
          if (!isNaN(depStart.getTime()) && !isNaN(depEnd.getTime())) {
            // Build route string
            const routeFrom = flight.departure_from_city || flight.departure_from || '';
            const routeTo = flight.departure_to_city || flight.departure_to || '';
            const route = routeFrom && routeTo ? `${routeFrom} → ${routeTo}` : '';
            
            // Build title - prefer departure_name, fallback to route
            let title = flight.departure_name || (route ? `Flight: ${route}` : 'Flight');
            
            // Build structured description
            let description = '';
            
            if (flight.departure_airline && flight.departure_flightnumber) {
              description += `✈️ ${flight.departure_airline} ${flight.departure_flightnumber}\n`;
            }
            
            if (route) {
              description += `Route: ${route}\n`;
            }
            
            // Personnel
            if (flight.personnel && flight.personnel.personnel_name && Array.isArray(flight.personnel.personnel_name) && flight.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              flight.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (flight.confirmation || flight.flight_status) {
              description += `\n📋 Booking Details:\n`;
              if (flight.confirmation) {
                description += `   Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                description += `   Status: ${flight.flight_status}\n`;
              }
            }

            const location = flight.departure_from_city 
              ? `${flight.departure_from} (${flight.departure_from_city})`
              : flight.departure_from || '';

            // URL: use notion_url if available
            const url = flight.notion_url || '';

            allCalendarEvents.push({
              start: depStart,
              end: depEnd,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'flight_departure'
            });
          }
        }

        // Return flight
        if (flight.return_time && flight.return_arrival_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const retTimes = parseUnifiedDateTime(flight.return_time);
          const retEndTimes = parseUnifiedDateTime(flight.return_arrival_time);
          const retStart = retTimes ? retTimes.start : new Date(flight.return_time);
          const retEnd = retEndTimes ? retEndTimes.start : new Date(flight.return_arrival_time);
          
          if (!isNaN(retStart.getTime()) && !isNaN(retEnd.getTime())) {
            // Build route string
            const routeFrom = flight.return_from_city || flight.return_from || '';
            const routeTo = flight.return_to_city || flight.return_to || '';
            const route = routeFrom && routeTo ? `${routeFrom} → ${routeTo}` : '';
            
            // Build title - prefer return_name, fallback to route
            let title = flight.return_name || (route ? `Flight Return: ${route}` : 'Flight Return');
            
            // Build structured description
            let description = '';
            
            if (flight.return_airline && flight.return_flightnumber) {
              description += `✈️ ${flight.return_airline} ${flight.return_flightnumber}\n`;
            }
            
            if (route) {
              description += `Route: ${route}\n`;
            }
            
            // Personnel
            if (flight.personnel && flight.personnel.personnel_name && Array.isArray(flight.personnel.personnel_name) && flight.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              flight.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (flight.return_confirmation || flight.confirmation || flight.flight_status) {
              description += `\n📋 Booking Details:\n`;
              if (flight.return_confirmation) {
                description += `   Confirmation: ${flight.return_confirmation}\n`;
              } else if (flight.confirmation) {
                description += `   Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                description += `   Status: ${flight.flight_status}\n`;
              }
            }

            const location = flight.return_from_city 
              ? `${flight.return_from} (${flight.return_from_city})`
              : flight.return_from || '';

            // URL: use notion_url if available
            const url = flight.notion_url || '';

            allCalendarEvents.push({
              start: retStart,
              end: retEnd,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'flight_return'
            });
          }
        }
      });
    }

    // Process hotels
    if (travelGroup.hotels && Array.isArray(travelGroup.hotels)) {
      travelGroup.hotels.forEach(hotel => {
        // Use travel group notion_url if hotel doesn't have one
        if (!hotel.notion_url && travelGroup.notion_url) {
          hotel.notion_url = travelGroup.notion_url;
        }
        // Extract location from title (e.g., "Hotel - North Beach ()" -> "North Beach")
        let locationName = '';
        if (hotel.title) {
          const titleMatch = hotel.title.match(/Hotel\s*-\s*([^(]+)/);
          if (titleMatch) {
            locationName = titleMatch[1].trim();
          }
        }
        
        // Build better title: "Hotel: [Hotel Name]" or "Hotel - [Location]: [Hotel Name]"
        let title = '';
        if (hotel.hotel_name) {
          if (locationName) {
            title = `Hotel - ${locationName}: ${hotel.hotel_name}`;
          } else {
            title = `Hotel: ${hotel.hotel_name}`;
          }
        } else if (locationName) {
          title = `Hotel - ${locationName}`;
        } else {
          title = hotel.title || 'Hotel';
        }
        
        // Hotel check-in
        if (hotel.check_in) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const checkInTimes = parseUnifiedDateTime(hotel.check_in);
          const checkIn = checkInTimes ? checkInTimes.start : new Date(hotel.check_in);
          if (!isNaN(checkIn.getTime())) {
            let description = '';
            
            // Hotel name and location
            if (hotel.hotel_name) {
              description += `🏨 ${hotel.hotel_name}\n`;
            }
            if (locationName) {
              description += `📍 ${locationName}\n`;
            }
            
            // Personnel
            if (hotel.personnel && hotel.personnel.personnel_name && Array.isArray(hotel.personnel.personnel_name) && hotel.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              hotel.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            // Booking details section
            const hasBookingDetails = hotel.confirmation || hotel.name_under_reservation || hotel.hotel_phone;
            if (hasBookingDetails) {
              description += `\n📋 Booking Details:\n`;
              if (hotel.confirmation) {
                description += `   Confirmation: ${hotel.confirmation}\n`;
              }
              if (hotel.name_under_reservation) {
                description += `   Reservation: ${hotel.name_under_reservation}\n`;
              }
              if (hotel.hotel_phone) {
                description += `   Phone: ${hotel.hotel_phone}\n`;
              }
            }
            
            // Dates section
            if (hotel.check_out) {
              const checkOutForDesc = parseUnifiedDateTime(hotel.check_out);
              const checkOutDesc = checkOutForDesc ? checkOutForDesc.start : new Date(hotel.check_out);
              if (!isNaN(checkOutDesc.getTime())) {
                const nights = Math.ceil((checkOutDesc - checkIn) / (1000 * 60 * 60 * 24));
                description += `\n📅 Dates:\n`;
                description += `   Check-out: ${checkOutDesc.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}\n`;
                description += `   Duration: ${nights} night${nights !== 1 ? 's' : ''}\n`;
              }
            }
            
            // Maps section (only if we have maps links)
            if (hotel.hotel_apple_maps || hotel.hotel_google_maps) {
              description += `\n🗺️ Maps:\n`;
              if (hotel.hotel_apple_maps) {
                description += `   Apple Maps: ${hotel.hotel_apple_maps}\n`;
              }
              if (hotel.hotel_google_maps) {
                description += `   Google Maps: ${hotel.hotel_google_maps}\n`;
              }
            }

            // Use actual check-in and check-out times
            const checkOutParsed = hotel.check_out ? parseUnifiedDateTime(hotel.check_out) : null;
            const checkOutDate = checkOutParsed ? checkOutParsed.start : (hotel.check_out ? new Date(hotel.check_out) : new Date(checkIn.getTime() + 24 * 60 * 60 * 1000));
            
            // Location field: combine hotel name and address
            let location = '';
            if (hotel.hotel_name && hotel.hotel_address) {
              location = `${hotel.hotel_name} ${hotel.hotel_address}`;
            } else if (hotel.hotel_address) {
              location = hotel.hotel_address;
            } else if (hotel.hotel_name) {
              location = hotel.hotel_name;
            }

            // URL: always use notion_url if available (maps links are in description, not URL field)
            const url = hotel.notion_url || '';

            allCalendarEvents.push({
              start: checkIn,
              end: checkOutDate,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'hotel_checkin'
            });
          }
        }

        // Hotel check-out
        if (hotel.check_out) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const checkOutTimes = parseUnifiedDateTime(hotel.check_out);
          const checkOut = checkOutTimes ? checkOutTimes.start : new Date(hotel.check_out);
          if (!isNaN(checkOut.getTime())) {
            let description = '';
            
            if (hotel.hotel_name) {
              description += `🏨 ${hotel.hotel_name} - Check-out\n`;
            }
            
            // Personnel
            if (hotel.personnel && hotel.personnel.personnel_name && Array.isArray(hotel.personnel.personnel_name) && hotel.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              hotel.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (hotel.confirmation) {
              description += `\n📋 Confirmation: ${hotel.confirmation}\n`;
            }

            // Location field: combine hotel name and address
            let location = '';
            if (hotel.hotel_name && hotel.hotel_address) {
              location = `${hotel.hotel_name} ${hotel.hotel_address}`;
            } else if (hotel.hotel_address) {
              location = hotel.hotel_address;
            } else if (hotel.hotel_name) {
              location = hotel.hotel_name;
            }

            // URL: always use notion_url if available (maps links are in description, not URL field)
            const url = hotel.notion_url || '';

            allCalendarEvents.push({
              start: checkOut,
              end: new Date(checkOut.getTime() + 60 * 60 * 1000), // 1 hour event
              title: hotel.hotel_name ? `${hotel.hotel_name} - Check-out` : 'Hotel Check-out',
              description: description.trim(),
              location: location,
              url: url,
              type: 'hotel_checkout'
            });
          }
        }
      });
    }

    // Process ground transportation
    if (travelGroup.ground_transportation && Array.isArray(travelGroup.ground_transportation)) {
      travelGroup.ground_transportation.forEach(transport => {
        // Pickup event
        if (transport.pickup_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const pickupTimes = parseUnifiedDateTime(transport.pickup_time);
          const pickupTime = pickupTimes ? pickupTimes.start : new Date(transport.pickup_time);
          if (!isNaN(pickupTime.getTime())) {
            let description = '';
            
            if (transport.transportation_name) {
              description += `🚗 ${transport.transportation_name}\n`;
            }
            
            if (transport.pickup_name) {
              description += `Pickup: ${transport.pickup_name}\n`;
            }
            
            // Personnel
            if (transport.personnel && transport.personnel.personnel_name && Array.isArray(transport.personnel.personnel_name) && transport.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              transport.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (transport.confirmation) {
              description += `\n📋 Confirmation: ${transport.confirmation}\n`;
            }
            
            if (transport.pickup_address) {
              description += `\n📍 ${transport.pickup_address}`;
            }
            
            if (transport.pickup_address_apple) {
              description += `\n🗺️ Apple Maps: ${transport.pickup_address_apple}`;
            }
            
            if (transport.pickup_address_google) {
              description += `\n🗺️ Google Maps: ${transport.pickup_address_google}`;
            }

            const location = transport.pickup_name || transport.pickup_address || '';
            const url = transport.notion_url || transport.pickup_address_google || transport.pickup_address_apple || '';

            allCalendarEvents.push({
              start: pickupTime,
              end: new Date(pickupTime.getTime() + 30 * 60 * 1000), // 30 minute event
              title: transport.transportation_name || `Transportation Pickup: ${transport.pickup_name || 'Pickup'}`,
              description: description.trim(),
              location: location,
              url: url,
              type: 'transportation_pickup'
            });
          }
        }

        // Drop-off event
        if (transport.drop_off_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const dropOffTimes = parseUnifiedDateTime(transport.drop_off_time);
          const dropOffTime = dropOffTimes ? dropOffTimes.start : new Date(transport.drop_off_time);
          if (!isNaN(dropOffTime.getTime())) {
            let description = '';
            
            if (transport.transportation_name) {
              description += `🚗 ${transport.transportation_name} - Drop-off\n`;
            }
            
            if (transport.drop_off_name) {
              description += `Drop-off: ${transport.drop_off_name}\n`;
            }
            
            // Personnel
            if (transport.personnel && transport.personnel.personnel_name && Array.isArray(transport.personnel.personnel_name) && transport.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              transport.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (transport.confirmation) {
              description += `\n📋 Confirmation: ${transport.confirmation}\n`;
            }
            
            if (transport.drop_off_address) {
              description += `\n📍 ${transport.drop_off_address}`;
            }

            const location = transport.drop_off_name || transport.drop_off_address || '';
            const url = transport.notion_url || '';

            allCalendarEvents.push({
              start: dropOffTime,
              end: new Date(dropOffTime.getTime() + 30 * 60 * 1000), // 30 minute event
              title: transport.transportation_name ? `${transport.transportation_name} - Drop-off` : `Transportation Drop-off: ${transport.drop_off_name || 'Drop-off'}`,
              description: description.trim(),
              location: location,
              url: url,
              type: 'transportation_dropoff'
            });
          }
        }
      });
    }
  });

  return allCalendarEvents;
}

// ============================================
// BLOCKOUT CALENDAR FUNCTIONS
// ============================================

// Helper function to get blockout calendar data by page ID
async function getBlockoutCalendarData() {
  if (!BLOCKOUT_CALENDAR_PAGE_ID) {
    throw new Error('BLOCKOUT_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = BLOCKOUT_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Blockout Admin property
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Blockout Admin property
  let blockoutEventsString = page.properties['Blockout Admin']?.formula?.string || 
                            page.properties['Blockout Admin']?.rich_text?.[0]?.text?.content ||
                            '[]';

  // Clean the string - remove any leading/trailing whitespace
  blockoutEventsString = blockoutEventsString.trim();

  // Try to extract JSON if there's extra text (look for first [ and last ])
  if (blockoutEventsString.includes('[') && blockoutEventsString.includes(']')) {
    const firstBracket = blockoutEventsString.indexOf('[');
    const lastBracket = blockoutEventsString.lastIndexOf(']');
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      blockoutEventsString = blockoutEventsString.substring(firstBracket, lastBracket + 1);
    }
  }

  // Fix double commas (common JSON formatting issue)
  blockoutEventsString = blockoutEventsString.replace(/,,+/g, ',');

  try {
    const blockoutEvents = JSON.parse(blockoutEventsString);
    return Array.isArray(blockoutEvents) ? blockoutEvents : [];
  } catch (e) {
    console.error('Error parsing Blockout Admin JSON. First 200 chars:', blockoutEventsString?.substring(0, 200));
    console.error('Full length:', blockoutEventsString?.length);
    console.error('Parse error:', e.message);
    throw new Error(`Blockout Admin JSON parse error: ${e.message}. First 200 chars: ${blockoutEventsString?.substring(0, 200)}`);
  }
}

// Helper function to process blockout events into calendar format
function processBlockoutEvents(eventsArray) {
  const allCalendarEvents = [];

  eventsArray.forEach(event => {
    // Blockout events have: personnel_name, date_start, date_end, reason, notion_url
    if (event.personnel_name && event.date_start && event.date_end) {
      try {
        // Parse dates (format: YYYY-MM-DD)
        const startDate = new Date(event.date_start + 'T00:00:00');
        const endDate = new Date(event.date_end + 'T23:59:59');
        
        // If end date is same as start date, make it a single day event
        if (event.date_start === event.date_end) {
          endDate.setHours(23, 59, 59);
        }
        
        if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          // Description: Only include reason if available
          // (Personnel name is in title, Notion URL is in URL field)
          let description = '';
          if (event.reason && event.reason.trim()) {
            description = event.reason.trim();
          }

          // Title: "Blockout: [Personnel Name]"
          const title = `Blockout: ${event.personnel_name}`;

          allCalendarEvents.push({
            start: startDate,
            end: endDate,
            title: title,
            description: description,
            location: '',
            url: event.notion_url || '',
            type: 'blockout_event'
          });
        }
      } catch (dateError) {
        console.error('Error parsing blockout event dates:', dateError, event);
      }
    }
  });

  return allCalendarEvents;
}

// Debug endpoint for blockout calendar
app.get('/debug/blockout', async (req, res) => {
  try {
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    // Format the page ID properly
    let pageId = BLOCKOUT_CALENDAR_PAGE_ID;
    if (pageId.length === 32 && !pageId.includes('-')) {
      pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Fetch the page
    const page = await retryNotionCall(() => 
      notion.pages.retrieve({ page_id: pageId })
    );

    // Get all properties to see what's available
    const availableProperties = Object.keys(page.properties || {});
    
    // Try to get Blockout Admin property
    const blockoutAdminProperty = page.properties['Blockout Admin'];
    
    // Get the raw value
    let blockoutEventsString = '';
    let propertyType = 'unknown';
    
    if (blockoutAdminProperty) {
      propertyType = blockoutAdminProperty.type;
      if (blockoutAdminProperty.formula?.type === 'string') {
        blockoutEventsString = blockoutAdminProperty.formula.string || '';
      } else if (blockoutAdminProperty.rich_text?.length > 0) {
        blockoutEventsString = blockoutAdminProperty.rich_text[0].text?.content || '';
      } else if (blockoutAdminProperty.formula) {
        blockoutEventsString = JSON.stringify(blockoutAdminProperty.formula);
      }
    }

    res.json({
      pageId: pageId,
      pageTitle: page.properties?.Name?.title?.[0]?.text?.content || 
                page.properties?.Title?.title?.[0]?.text?.content ||
                'Unknown',
      availableProperties: availableProperties,
      blockoutAdminFound: !!blockoutAdminProperty,
      blockoutAdminType: propertyType,
      blockoutAdminValueLength: blockoutEventsString?.length || 0,
      blockoutAdminPreview: blockoutEventsString?.substring(0, 500) || 'Empty or not found',
      fullBlockoutAdminValue: blockoutEventsString || null
    });
  } catch (error) {
    console.error('Error debugging blockout calendar:', error);
    res.status(500).json({ 
      error: 'Error debugging blockout calendar',
      message: error.message,
      details: error.stack
    });
  }
});

// Health check endpoint
app.get('/', (_req, res) => {
  res.json({
    status: `Calendar Feed Server Running (Cache ${cacheEnabled ? 'Enabled' : 'Disabled'})`,
    endpoints: {
      subscribe: '/subscribe/:personId',
      calendar: '/calendar/:personId',
      ics: '/calendar/:personId?format=ics',
      debug: '/debug/simple-test/:personId',
      debug_calendar_data: '/debug/calendar-data/:personId',
      cache_clear: '/cache/clear/:personId',
      cache_clear_all: '/cache/clear-all',
      regenerate_one: '/regenerate/:personId',
      regenerate_all: '/regenerate-all'
    },
    cache: {
      ttl: `${CACHE_TTL} seconds (2 hours)`,
      status: cacheEnabled ? 'enabled' : 'disabled'
    },
    backgroundJob: {
      status: 'running',
      interval: '5 minutes',
      description: 'Updates all people every 5 minutes (paginated, batched parallel)'
    }
  });
});

// Flight countdown API endpoint - Direct Notion Query
app.get('/api/flight/:flightId', async (req, res) => {
  try {
    const { flightId } = req.params;
    
    // Special case for test flight
    if (flightId === 'test') {
      return res.json({
        flightNumber: 'AS 1360',
        departureTime: '2025-01-15T19:59:00.000Z/2025-01-15T21:34:00.000Z',
        airline: 'Alaska',
        route: 'LAX-SJD',
        confirmation: 'TEST123',
        departureCode: 'LAX',
        arrivalCode: 'SJD',
        departureName: 'Los Angeles International Airport',
        arrivalName: 'Los Cabos International Airport'
      });
    }
    
    // Parse flightId: {notionPageId}-{direction}
    const parts = flightId.split('-');
    if (parts.length < 2) {
      return res.status(400).json({ error: 'Invalid flight ID format' });
    }
    
    const direction = parts.pop();
    let notionPageId = parts.join('-');
    
    // Convert 32-character page ID to UUID format if needed
    if (notionPageId.length === 32 && !notionPageId.includes('-')) {
      notionPageId = notionPageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }
    
    if (!['departure', 'return'].includes(direction)) {
      return res.status(400).json({ error: 'Invalid direction. Must be "departure" or "return"' });
    }
    
    // Query Notion page
    const page = await notion.pages.retrieve({ page_id: notionPageId });
    const properties = page.properties;
    
    // Debug: Log available properties
    console.log('Available properties:', Object.keys(properties));
    
    // Temporary: Return available properties for debugging
    if (req.query.debug === 'true') {
      return res.json({
        notionPageId,
        direction,
        availableProperties: Object.keys(properties),
        properties: properties
      });
    }
    
    // Extract flight data based on direction
    let flightData;
    if (direction === 'departure') {
      // Parse departure time range
      const departureTime = properties.departure_time?.date?.start;
      const departureArrivalTime = properties.departure_arrival_time?.date?.start;
      
      if (!departureTime) {
        return res.status(404).json({ error: 'Departure time not found' });
      }
      
      const departureTimeRange = departureArrivalTime
        ? `${departureTime}/${departureArrivalTime}`
        : `${departureTime}/${departureTime}`;
      
      flightData = {
        flightNumber: properties.departure_flightnumber?.title?.[0]?.text?.content || 'N/A',
        departureTime: departureTimeRange,
        airline: properties.departure_airline?.select?.name || 'N/A',
        route: `${properties.departure_airport?.select?.name || 'N/A'}-${properties.return_airport?.select?.name || 'N/A'}`,
        confirmation: properties.confirmation?.rich_text?.[0]?.text?.content || 'N/A',
        departureCode: properties.departure_airport?.select?.name || 'N/A',
        arrivalCode: properties.return_airport?.select?.name || 'N/A',
        departureName: properties.departure_airport_name?.rich_text?.[0]?.text?.content || 'N/A',
        arrivalName: properties.return_airport_name?.rich_text?.[0]?.text?.content || 'N/A'
      };
    } else {
      // Parse return time range
      const returnTime = properties.return_time?.date?.start;
      const returnArrivalTime = properties.return_arrival_time?.date?.start;
      
      if (!returnTime) {
        return res.status(404).json({ error: 'Return time not found' });
      }
      
      const returnTimeRange = returnArrivalTime
        ? `${returnTime}/${returnArrivalTime}`
        : `${returnTime}/${returnTime}`;
      
      flightData = {
        flightNumber: properties.return_flightnumber?.title?.[0]?.text?.content || 'N/A',
        departureTime: returnTimeRange,
        airline: properties.return_airline?.select?.name || properties.departure_airline?.select?.name || 'N/A',
        route: `${properties.return_airport?.select?.name || 'N/A'}-${properties.departure_airport?.select?.name || 'N/A'}`,
        confirmation: properties.confirmation?.rich_text?.[0]?.text?.content || 'N/A',
        departureCode: properties.return_airport?.select?.name || 'N/A',
        arrivalCode: properties.departure_airport?.select?.name || 'N/A',
        departureName: properties.return_airport_name?.rich_text?.[0]?.text?.content || 'N/A',
        arrivalName: properties.departure_airport_name?.rich_text?.[0]?.text?.content || 'N/A'
      };
    }
    
    res.json(flightData);
  } catch (error) {
    console.error('Flight API error:', error);
    
    // Handle specific Notion API errors
    if (error.code === 'object_not_found') {
      return res.status(404).json({ error: 'Flight not found' });
    }
    
    res.status(500).json({ error: 'Internal server error' });
  }
});

// FlightAware real-time status endpoint
app.get('/api/flight/:flightId/status', async (req, res) => {
  try {
    const { flightId } = req.params;
    
    // Special case for test flight - return no data to hide the section
    if (flightId === 'test') {
      return res.json({
        status: 'No Data',
        message: 'Test flight - no real-time data available',
        lastUpdated: new Date().toISOString(),
        source: 'test'
      });
    }
    
    // Check if this is a flight ident (like "AS271") or a Notion page ID
    if (flightId.includes('-') && flightId.split('-').length >= 2) {
      // This is a Notion page ID format: {notionPageId}-{direction}
      const parts = flightId.split('-');
      const direction = parts.pop();
      let notionPageId = parts.join('-');
      
      // Convert 32-character page ID to UUID format if needed
      if (notionPageId.length === 32 && !notionPageId.includes('-')) {
        notionPageId = notionPageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
      }
      
      if (!['departure', 'return'].includes(direction)) {
        return res.status(400).json({ error: 'Invalid direction. Must be "departure" or "return"' });
      }
    } else {
      // This is a flight ident (like "AS271") - we can't get Notion data, so return early
      return res.json({
        status: 'No Data',
        message: 'Flight ident provided - cannot fetch Notion data',
        lastUpdated: new Date().toISOString(),
        source: 'flight_ident'
      });
    }
    
    // Get basic flight data from Notion first
    const page = await notion.pages.retrieve({ page_id: notionPageId });
    const properties = page.properties;
    
    let airline, flightNumber, departureDate;
    
    if (direction === 'departure') {
      airline = properties.departure_airline?.select?.name;
      flightNumber = properties.departure_flightnumber?.title?.[0]?.text?.content;
      departureDate = properties.departure_time?.date?.start;
    } else {
      airline = properties.return_airline?.select?.name || properties.departure_airline?.select?.name;
      flightNumber = properties.return_flightnumber?.title?.[0]?.text?.content;
      departureDate = properties.return_time?.date?.start;
    }
    
    if (!airline || !flightNumber || !departureDate) {
      return res.status(400).json({ error: 'Missing required flight information' });
    }
    
    // Check if we should fetch real-time data (within 24 hours of departure)
    const now = new Date();
    const depDate = new Date(departureDate);
    const hoursUntilDeparture = (depDate - now) / (1000 * 60 * 60);
    
    // Only fetch real-time data if within 24 hours of departure and not more than 2 hours past arrival
    if (hoursUntilDeparture > 24 || hoursUntilDeparture < -2) {
      return res.json({
        status: 'Scheduled',
        message: 'Real-time tracking not available for this flight',
        lastUpdated: new Date().toISOString(),
        source: 'notion'
      });
    }
    
    // Create cache key
    const cacheKey = `flight-status:${airline}:${flightNumber}:${departureDate.split('T')[0]}`;
    
    // Check cache first
    if (cacheEnabled && redis) {
      try {
        const cached = await redis.get(cacheKey);
        if (cached) {
          const cachedData = JSON.parse(cached);
          console.log(`✅ Flight status cache hit for ${airline}${flightNumber}`);
          return res.json({
            ...cachedData,
            source: 'cache'
          });
        }
      } catch (cacheError) {
        console.warn('Cache read error:', cacheError.message);
      }
    }
    
    // Fetch from FlightAware API
    console.log(`🔄 Fetching real-time status for ${airline}${flightNumber}`);
    const flightStatus = await fetchFlightStatus(airline, flightNumber, departureDate);
    
    if (!flightStatus) {
      return res.json({
        status: 'No Data',
        message: 'Flight status not available from FlightAware',
        lastUpdated: new Date().toISOString(),
        source: 'notion'
      });
    }
    
    // Format the response
    const statusData = {
      status: flightStatus.status || 'Unknown',
      departureGate: flightStatus.origin_gate || null,
      departureTerminal: flightStatus.origin_terminal || null,
      arrivalGate: flightStatus.destination_gate || null,
      arrivalTerminal: flightStatus.destination_terminal || null,
      baggageClaim: flightStatus.baggage_claim || null,
      delay: flightStatus.delay || 0,
      estimatedDeparture: flightStatus.estimated_out || flightStatus.scheduled_out,
      estimatedArrival: flightStatus.estimated_in || flightStatus.scheduled_in,
      actualDeparture: flightStatus.actual_out,
      actualArrival: flightStatus.actual_in,
      lastUpdated: new Date().toISOString(),
      source: 'flightaware'
    };
    
    // Cache the result for 1 hour (3600 seconds)
    if (cacheEnabled && redis) {
      try {
        await redis.setex(cacheKey, 3600, JSON.stringify(statusData));
        console.log(`💾 Cached flight status for ${airline}${flightNumber}`);
      } catch (cacheError) {
        console.warn('Cache write error:', cacheError.message);
      }
    }
    
    res.json(statusData);
    
  } catch (error) {
    console.error('Flight status API error:', error);
    
    // Handle specific errors
    if (error.message === 'FlightAware API key not configured') {
      return res.status(503).json({ 
        error: 'Flight tracking service not configured',
        message: 'Real-time flight status is not available'
      });
    }
    
    if (error.response?.status === 401) {
      return res.status(503).json({ 
        error: 'Flight tracking service authentication failed',
        message: 'Real-time flight status is not available'
      });
    }
    
    if (error.response?.status === 429) {
      return res.status(503).json({ 
        error: 'Flight tracking service rate limit exceeded',
        message: 'Please try again later'
      });
    }
    
    // For other errors, return a fallback response
    res.json({
      status: 'Error',
      message: 'Unable to fetch real-time status',
      lastUpdated: new Date().toISOString(),
      source: 'error'
    });
  }
});

// Cache management endpoint - clear cache for a specific person
app.get('/cache/clear/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }
    
    // Clear both ICS and JSON cache
    const icsKey = `calendar:${personId}:ics`;
    const jsonKey = `calendar:${personId}:json`;
    
    const icsDeleted = await redis.del(icsKey);
    const jsonDeleted = await redis.del(jsonKey);
    
    res.json({
      success: true,
      message: 'Cache cleared successfully',
      personId: personId,
      cleared: {
        ics: icsDeleted > 0,
        json: jsonDeleted > 0
      }
    });
  } catch (error) {
    console.error('Cache clear error:', error);
    res.status(500).json({ error: 'Error clearing cache' });
  }
});

// Cache management endpoint - clear all caches
app.get('/cache/clear-all', async (req, res) => {
  try {
    // Get all calendar cache keys
    const keys = await redis.keys('calendar:*');
    
    if (keys.length === 0) {
      return res.json({
        success: true,
        message: 'No cache entries found',
        cleared: 0
      });
    }
    
    // Delete all cache keys
    const deleted = await redis.del(keys);
    
    res.json({
      success: true,
      message: 'All caches cleared successfully',
      cleared: deleted
    });
  } catch (error) {
    console.error('Cache clear all error:', error);
    res.status(500).json({ error: 'Error clearing all caches' });
  }
});

// Debug endpoint to explore Calendar Data database
app.get('/debug/calendar-data/:personId', async (req, res) => {
  try {
    let { personId } = req.params;

    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    if (!CALENDAR_DATA_DB) {
      return res.status(500).json({ error: 'CALENDAR_DATA_DATABASE_ID not configured' });
    }

    // Query Calendar Data database for events related to this person
    const response = await notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    });

    // Return structured data for inspection
    const events = response.results.map(page => {
      const props = page.properties;
      return {
        id: page.id,
        name: props.Name?.title?.[0]?.text?.content || 'No name',
        personnel: props.Personnel?.relation || [],
        url: props.URL?.url || 'No URL',
        // Show the key event properties we need
        events: props.Events?.formula?.string || props.Events?.rich_text?.[0]?.text?.content || 'No events',
        flights: props.Flights?.formula?.string || props.Flights?.rich_text?.[0]?.text?.content || 'No flights',
        rehearsals: props.Rehearsals?.formula?.string || props.Rehearsals?.rich_text?.[0]?.text?.content || 'No rehearsals',
        transportation: props.Transportation?.formula?.string || props.Transportation?.rich_text?.[0]?.text?.content || 'No transportation',
        hotels: props.Hotels?.formula?.string || props.Hotels?.rich_text?.[0]?.text?.content || 'No hotels',
        teamCalendar: props['Team Calendar']?.formula?.string || props['Team Calendar']?.rich_text?.[0]?.text?.content || 'No team calendar',
        // Show all available properties for debugging
        allProperties: Object.keys(props)
      };
    });
    
    // Count actual events from all JSON arrays and extract calltime info
    let totalActualEvents = 0;
    let calltimeDebug = [];
    
    events.forEach(event => {
      try {
        const eventsArray = JSON.parse(event.events || '[]');
        const flightsArray = JSON.parse(event.flights || '[]');
        const rehearsalsArray = JSON.parse(event.rehearsals || '[]');
        const transportationArray = JSON.parse(event.transportation || '[]');
        const hotelsArray = JSON.parse(event.hotels || '[]');
        const teamCalendarArray = JSON.parse(event.teamCalendar || '[]');
        
        totalActualEvents += eventsArray.length + flightsArray.length + rehearsalsArray.length + 
                           transportationArray.length + hotelsArray.length + teamCalendarArray.length;
        
        // DEBUG: Extract calltime info for specific events
        eventsArray.forEach(evt => {
          if (evt.event_name && (evt.event_name.includes('Pacific Palisades') || evt.event_name.includes('11-15') || (evt.calltime && evt.calltime.includes('2025-11-15')))) {
            // Extract raw calltime from formula string before parsing - find the event by name and extract its calltime
            const escapedEventName = evt.event_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const eventMatch = (event.events || '').match(new RegExp(`"event_name":"[^"]*${escapedEventName.substring(0, 10)}[^"]*"[^}]*"calltime":"([^"]*)"`));
            calltimeDebug.push({
              event_name: evt.event_name,
              raw_calltime_from_formula_string: eventMatch ? eventMatch[1] : null,
              raw_calltime_from_parsed_json: evt.calltime,
              raw_event_date_from_formula: evt.event_date,
              calltime_type: typeof evt.calltime,
              calltime_length: evt.calltime ? evt.calltime.length : 0,
              full_event: evt
            });
          }
        });
      } catch (e) {
        console.warn('Error parsing JSON in debug endpoint:', e);
      }
    });
    
    res.json({
      personId: personId,
      totalDatabaseRows: response.results.length,
      totalActualEvents: totalActualEvents,
      calltimeDebug: calltimeDebug,
      events: events
    });
  } catch (error) {
    console.error('Calendar Data debug error:', error);
    res.status(500).json({ error: 'Error querying Calendar Data', details: error.message });
  }
});

// Regeneration endpoint - regenerate calendar for a specific person
app.get('/regenerate/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    const result = await regenerateCalendarForPerson(personId);
    
    if (result.success) {
      res.json({
        success: true,
        message: `Calendar regenerated successfully for ${result.personName}`,
        personId: result.personId,
        personName: result.personName,
        eventCount: result.eventCount,
        cacheTTL: CACHE_TTL
      });
    } else {
      res.status(404).json({
        success: false,
        message: 'Failed to regenerate calendar',
        personId: result.personId,
        reason: result.reason || result.error
      });
    }
  } catch (error) {
    console.error('Regeneration endpoint error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Error regenerating calendar',
      details: error.message
    });
  }
});

// Status endpoint to check regeneration progress
app.get('/regenerate/:personId/status', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    if (redis && cacheEnabled) {
      const status = await redis.get(`regenerate:status:${personId}`);
      if (status) {
        return res.json({
          success: true,
          personId: personId,
          status: status,
          message: status === 'in_progress' ? 'Regeneration in progress' :
                   status === 'completed' ? 'Regeneration completed' :
                   status === 'failed' ? 'Regeneration failed' : 'Unknown status'
        });
      }
    }

    // If no status found, check if calendar exists in cache
    const cacheKey = `calendar:${personId}:ics`;
    if (redis && cacheEnabled) {
      const cached = await redis.exists(cacheKey);
      if (cached) {
        return res.json({
          success: true,
          personId: personId,
          status: 'not_regenerating',
          message: 'Calendar is cached. No regeneration in progress.',
          cached: true
        });
      }
    }

    res.json({
      success: true,
      personId: personId,
      status: 'unknown',
      message: 'No regeneration status found. Calendar may not be cached.'
    });
  } catch (error) {
    console.error('Status endpoint error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Error checking regeneration status',
      details: error.message
    });
  }
});

// Regeneration endpoint - regenerate all calendars
app.get('/regenerate-all', async (req, res) => {
  try {
    // Start the regeneration process
    res.json({
      success: true,
      message: 'Bulk calendar regeneration started',
      note: 'This will take several minutes. Check server logs for progress.'
    });
    
    // Run regeneration in the background (don't await)
    regenerateAllCalendars().then(result => {
      console.log('Bulk regeneration completed:', result);
    }).catch(error => {
      console.error('Bulk regeneration failed:', error);
    });
    
  } catch (error) {
    console.error('Regeneration all endpoint error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Error starting bulk regeneration',
      details: error.message
    });
  }
});

// Simple formula test endpoint
app.get('/debug/simple-test/:personId', async (req, res) => {
  try {
    let { personId } = req.params;

    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Get person from Personnel database
    const person = await notion.pages.retrieve({ page_id: personId });
    
    if (!person) {
      return res.status(404).json({ error: 'Person not found' });
    }

    // Test multiple properties to see what works
    const testResults = {
      personId: personId,
      fullName: person.properties?.['Full Name']?.formula?.string,
      timestamp: new Date().toISOString(),
      // Test if we can get Gig Payroll count
      gigPayrollCount: person.properties?.['Gig Payroll']?.relation?.length || 0,
      availableProperties: Object.keys(person.properties || {}),
      // Try to get the Calendar Feed JSON
      calendarFeedExists: !!person.properties?.['Calendar Feed JSON'],
      calendarFeedType: person.properties?.['Calendar Feed JSON']?.type,
      calendarFeedLength: person.properties?.['Calendar Feed JSON']?.formula?.string?.length || 0,
      calendarFeedPreview: person.properties?.['Calendar Feed JSON']?.formula?.string?.substring(0, 200) || 'null',
      // Also test the "Test Calendar Feed" property
      testCalendarFeedExists: !!person.properties?.['Test Calendar Feed'],
      testCalendarFeedLength: person.properties?.['Test Calendar Feed']?.formula?.string?.length || 0,
      testCalendarFeedPreview: person.properties?.['Test Calendar Feed']?.formula?.string?.substring(0, 200) || 'null'
    };

    // Debug logging removed for performance

    res.json(testResults);
  } catch (error) {
    console.error('Simple test error:', error);
    res.status(500).json({ error: 'Error in simple test', details: error.message });
  }
});

// Calendar subscription endpoint with proper headers
// Admin calendar subscription page
app.get('/subscribe/admin', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL like /subscribe/admin%20%20...)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/admin' && originalPath.startsWith('/subscribe/admin')) {
    return res.redirect(301, '/subscribe/admin');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/admin`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/admin.ics');
    }
    
    // For web browsers, show a subscription page with same styling as personal calendars
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as personal calendars but with admin-specific content
    const adminSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Admin Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .copy-btn.copied {
            background: #2ecc71;
            color: #000;
            border-color: #2ecc71;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Admin Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all upcoming events across all personnel in your calendar app. Includes event details, venues, personnel, general info, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/admin" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible">
                <div class="collapsible-header" onclick="toggleCollapsible()">
                    Other Calendar Apps (Outlook, etc.)
                </div>
                <div class="collapsible-content" id="collapsibleContent">
                    <div class="collapsible-inner">
                        <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                        <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                        <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                        <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                            <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                            <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const header = event.currentTarget;
            const content = document.getElementById('collapsibleContent');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `;
    
    res.send(adminSubscriptionPage);
  } catch (error) {
    console.error('Error loading admin subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

app.get('/subscribe/travel', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL like /subscribe/travel%20%20...)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/travel' && originalPath.startsWith('/subscribe/travel')) {
    return res.redirect(301, '/subscribe/travel');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/travel`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/travel.ics');
    }
    
    // For web browsers, show a subscription page with same styling as personal calendars
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as personal calendars but with travel-specific content
    const travelSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Travel Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .copy-btn.copied {
            background: #2ecc71;
            color: #000;
            border-color: #2ecc71;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        .collapsible {
            margin-top: 20px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
            border-color: #333;
        }
        
        .collapsible-header::after {
            content: '▼';
            font-size: 0.7rem;
            transition: transform 0.3s ease;
        }
        
        .collapsible-header.active::after {
            transform: rotate(180deg);
        }
        
        .collapsible-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }
        
        .collapsible-content.active {
            max-height: 500px;
        }
        
        .collapsible-inner {
            background: #141414;
            border: 1px solid #2a2a2a;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 24px;
            margin-top: -8px;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Travel Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all travel events across all personnel in your calendar app. Includes flight details, hotel information, travel dates, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/travel" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible">
                <div class="collapsible-header" onclick="toggleCollapsible()">
                    Other Calendar Apps (Outlook, etc.)
                </div>
                <div class="collapsible-content" id="collapsibleContent">
                    <div class="collapsible-inner">
                        <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                        <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                        <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                        <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                            <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                            <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const header = event.currentTarget;
            const content = document.getElementById('collapsibleContent');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `;
    
    res.send(travelSubscriptionPage);
  } catch (error) {
    console.error('Error loading travel subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

// Blockout calendar subscription page
app.get('/subscribe/blockout', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/blockout' && originalPath.startsWith('/subscribe/blockout')) {
    return res.redirect(301, '/subscribe/blockout');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/blockout`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/blockout.ics');
    }
    
    // For web browsers, show a subscription page
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as travel calendar but with blockout-specific content
    const blockoutSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Blockout Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            width: 100%;
            font-weight: 500;
        }
        
        .copy-btn:hover { 
            background: #222; 
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Blockout Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all blockout events in your calendar app. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/blockout" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible-header" onclick="toggleCollapsible()">
                Other Calendar Apps (Outlook, etc.)
            </div>
            <div class="collapsible-content" id="collapsibleContent" style="max-height: 0; overflow: hidden; transition: max-height 0.3s ease;">
                <div style="padding: 24px; background: #0a0a0a; border: 1px solid #2a2a2a; border-top: none; border-radius: 0 0 8px 8px; margin-top: -8px;">
                    <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                    <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                    <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                    <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                        <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                        <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                    </p>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const content = document.getElementById('collapsibleContent');
            if (content.style.maxHeight === '0px' || !content.style.maxHeight) {
                content.style.maxHeight = '500px';
            } else {
                content.style.maxHeight = '0px';
            }
        }
    </script>
</body>
</html>
    `;
    
    res.send(blockoutSubscriptionPage);
  } catch (error) {
    console.error('Error loading blockout subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

app.get('/subscribe/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Get person name from URL query parameter only
    const personName = req.query.name || null;
    
    const subscriptionUrl = `https://${req.get('host')}/calendar/${personId}.ics`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, `/calendar/${personId}`);
    }
    
    // For web browsers, show a subscription page with instructions
    res.setHeader('Content-Type', 'text/html');
    res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Downbeat Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .collapsible {
            margin-top: 20px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
            border-color: #333;
        }
        
        .collapsible-header::after {
            content: '▼';
            font-size: 0.7rem;
            transition: transform 0.3s ease;
        }
        
        .collapsible-header.active::after {
            transform: rotate(180deg);
        }
        
        .collapsible-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }
        
        .collapsible-content.active {
            max-height: 500px;
        }
        
        .collapsible-inner {
            background: #141414;
            border: 1px solid #2a2a2a;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 24px;
            margin-top: -8px;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
            font-weight: 500;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .divider {
            color: #444;
            font-size: 0.85rem;
            text-align: center;
            margin: 16px 0;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>${personName ? `Hello ${personName}!` : 'Subscribe to Calendar'}</h1>
            <div class="separator"></div>
            <div class="description"><strong>Introducing Downbeat Calendar</strong> - your personalized event calendar with everything you need: call times, venue details, MD contacts, payroll info, flights, hotels, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/${personId}" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="collapsible">
            <div class="collapsible-header" onclick="toggleCollapsible()">
                Other Calendar Apps (Outlook, etc.)
            </div>
            <div class="collapsible-content">
                <div class="collapsible-inner">
                    <p style="margin: 0 0 16px 0; color: #888; font-size: 0.9rem;">
                        Copy this URL and add it to your calendar app:
                    </p>
                    <div class="url-box" id="urlBox">${subscriptionUrl}</div>
                    <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                    <div class="divider">• • •</div>
                    <div class="step-text" style="margin-top: 16px;">
                        <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br><br>
                        <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2500);
        }
        
        function copyUrl() {
            const urlBox = document.getElementById('urlBox');
            navigator.clipboard.writeText(urlBox.textContent).then(() => {
                showToast();
            });
        }
        
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                // Small delay so user sees the toast before opening new tab
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/u/0/r/settings/addcalendar', '_blank', 'noopener,noreferrer');
                }, 300);
            });
        }
        
        function toggleCollapsible() {
            const header = document.querySelector('.collapsible-header');
            const content = document.querySelector('.collapsible-content');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `);
    
  } catch (error) {
    console.error('Subscription page error:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

// ============================================
// ADMIN CALENDAR ENDPOINT
// ============================================
app.get('/admin/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:admin:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          console.log(`✅ Cache HIT for admin calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="admin-calendar.ics"');
            return res.send(cachedData);
          }
        }
        console.log(`❌ Cache MISS for admin calendar (${format.toUpperCase()})`);
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if admin calendar is configured
    if (!ADMIN_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Admin calendar not configured',
        message: 'ADMIN_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Admin Calendar',
          description: 'Admin calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch admin calendar data
    let adminEvents;
    try {
      adminEvents = await getAdminCalendarData();
      
      if (!adminEvents || adminEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Admin Events property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Admin Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching admin calendar data:', error);
      
      const errorMsg = {
        error: 'Error fetching admin calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        const errorCalendar = ical({ 
          name: 'Admin Calendar',
          description: `Error: ${error.message}`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processAdminEvents(adminEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Admin Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, jsonData);
          console.log(`💾 Cached admin calendar JSON (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Admin Calendar',
        description: 'All upcoming events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: getAlarmsForEvent(event.type, event.title)
        });
      });
      
      const icsData = calendar.toString();
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, icsData);
          console.log(`💾 Cached admin calendar ICS (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="admin-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Admin calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating admin calendar',
      message: error.message
    });
  }
});

// Admin calendar regeneration endpoint (clears cache and regenerates)
app.get('/admin/calendar/regen', async (req, res) => {
  try {
    if (!ADMIN_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Admin calendar not configured',
        message: 'ADMIN_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating admin calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:admin:ics');
        await redis.del('calendar:admin:json');
        console.log('✅ Admin calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const adminEvents = await getAdminCalendarData();
    const allCalendarEvents = processAdminEvents(adminEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Admin Calendar',
      description: 'All upcoming events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });
    
    const icsData = calendar.toString();
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Admin Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await redis.setEx('calendar:admin:ics', CACHE_TTL, icsData);
        await redis.setEx('calendar:admin:json', CACHE_TTL, jsonData);
        console.log(`💾 Admin calendar regenerated and cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    // Return success response
    res.json({
      success: true,
      message: 'Admin calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cache_cleared: true,
      cached_for_seconds: CACHE_TTL
    });

  } catch (error) {
    console.error('Admin calendar regen error:', error);
    res.status(500).json({ 
      error: 'Error regenerating admin calendar',
      message: error.message
    });
  }
});

// ============================================
// TRAVEL CALENDAR ENDPOINTS
// ============================================

app.get('/travel/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:travel:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          console.log(`✅ Cache HIT for travel calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
            return res.send(cachedData);
          }
        }
        console.log(`❌ Cache MISS for travel calendar (${format.toUpperCase()})`);
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if travel calendar is configured
    if (!TRAVEL_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Travel calendar not configured',
        message: 'TRAVEL_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Travel Calendar',
          description: 'Travel calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch travel calendar data
    let travelEvents;
    try {
      travelEvents = await getTravelCalendarData();
      
      if (!travelEvents || travelEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Travel Admin property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Travel Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching travel calendar data:', error);
      
      // If Notion API times out, try to return cached data as fallback
      const isTimeout = error.message?.includes('504') || error.message?.includes('timeout') || error.message?.includes('Gateway Timeout');
      
      if (isTimeout && redis && cacheEnabled) {
        console.log('⚠️  Notion API timeout - attempting to return cached data...');
        try {
          const cachedData = await redis.get(cacheKey);
          if (cachedData) {
            console.log(`✅ Returning cached travel calendar data (fallback from timeout)`);
            if (format === 'json') {
              res.setHeader('Content-Type', 'application/json');
              return res.send(cachedData);
            } else {
              res.setHeader('Content-Type', 'text/calendar');
              res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
              return res.send(cachedData);
            }
          }
        } catch (cacheError) {
          console.error('Error retrieving cached data:', cacheError);
        }
      }
      
      const errorMsg = {
        error: 'Error fetching travel calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Always return a valid ICS file, even on error
        const errorCalendar = ical({ 
          name: 'Travel Calendar',
          description: `Error: ${error.message}. Please try again later.`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processTravelEvents(travelEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Travel Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, jsonData);
          console.log(`💾 Cached travel calendar JSON (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Travel Calendar',
        description: 'All travel events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: getAlarmsForEvent(event.type, event.title)
        });
      });
      
      const icsData = calendar.toString();
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, icsData);
          console.log(`💾 Cached travel calendar ICS (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Travel calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating travel calendar',
      message: error.message
    });
  }
});

// Travel calendar regeneration endpoint (clears cache and regenerates)
app.get('/travel/calendar/regen', async (req, res) => {
  try {
    if (!TRAVEL_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Travel calendar not configured',
        message: 'TRAVEL_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating travel calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:travel:ics');
        await redis.del('calendar:travel:json');
        console.log('✅ Travel calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const travelEvents = await getTravelCalendarData();
    const allCalendarEvents = processTravelEvents(travelEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Travel Calendar',
      description: 'All travel events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });
    
    const icsData = calendar.toString();
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Travel Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await redis.setEx('calendar:travel:ics', CACHE_TTL, icsData);
        await redis.setEx('calendar:travel:json', CACHE_TTL, jsonData);
        console.log(`💾 Travel calendar regenerated and cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    // Return success response
    res.json({
      success: true,
      message: 'Travel calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cache_cleared: true,
      cached_for_seconds: CACHE_TTL,
      debugLogs: getTravelDebugLogs()
    });

  } catch (error) {
    console.error('Travel calendar regen error:', error);
    res.status(500).json({ 
      error: 'Error regenerating travel calendar',
      message: error.message
    });
  }
});

// ============================================
// BLOCKOUT CALENDAR ENDPOINTS
// ============================================

app.get('/blockout/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:blockout:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          console.log(`✅ Cache HIT for blockout calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
            return res.send(cachedData);
          }
        }
        console.log(`❌ Cache MISS for blockout calendar (${format.toUpperCase()})`);
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if blockout calendar is configured
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Blockout Calendar',
          description: 'Blockout calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch blockout calendar data
    let blockoutEvents;
    try {
      blockoutEvents = await getBlockoutCalendarData();
      
      if (!blockoutEvents || blockoutEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Blockout Admin property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Blockout Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching blockout calendar data:', error);
      
      // If Notion API times out, try to return cached data as fallback
      const isTimeout = error.message?.includes('504') || error.message?.includes('timeout') || error.message?.includes('Gateway Timeout');
      
      if (isTimeout && redis && cacheEnabled) {
        console.log('⚠️  Notion API timeout - attempting to return cached data...');
        try {
          const cachedData = await redis.get(cacheKey);
          if (cachedData) {
            console.log(`✅ Returning cached blockout calendar data (fallback from timeout)`);
            if (format === 'json') {
              res.setHeader('Content-Type', 'application/json');
              return res.send(cachedData);
            } else {
              res.setHeader('Content-Type', 'text/calendar');
              res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
              return res.send(cachedData);
            }
          }
        } catch (cacheError) {
          console.error('Error retrieving cached data:', cacheError);
        }
      }
      
      const errorMsg = {
        error: 'Error fetching blockout calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Always return a valid ICS file, even on error
        const errorCalendar = ical({ 
          name: 'Blockout Calendar',
          description: `Error: ${error.message}. Please try again later.`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processBlockoutEvents(blockoutEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Blockout Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, jsonData);
          console.log(`💾 Cached blockout calendar JSON (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Blockout Calendar',
        description: 'All blockout events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: getAlarmsForEvent(event.type, event.title)
        });
      });
      
      const icsData = calendar.toString();
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, icsData);
          console.log(`💾 Cached blockout calendar ICS (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Blockout calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating blockout calendar',
      message: error.message
    });
  }
});

// Blockout calendar regeneration endpoint (clears cache and regenerates)
app.get('/blockout/calendar/regen', async (req, res) => {
  try {
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating blockout calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:blockout:ics');
        await redis.del('calendar:blockout:json');
        console.log('✅ Blockout calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const blockoutEvents = await getBlockoutCalendarData();
    const allCalendarEvents = processBlockoutEvents(blockoutEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Blockout Calendar',
      description: 'All blockout events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });
    
    const icsData = calendar.toString();
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Blockout Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await redis.setEx('calendar:blockout:ics', CACHE_TTL, icsData);
        await redis.setEx('calendar:blockout:json', CACHE_TTL, jsonData);
        console.log(`💾 Blockout calendar cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    res.json({
      success: true,
      message: 'Blockout calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cached: redis && cacheEnabled
    });
  } catch (error) {
    console.error('Error regenerating blockout calendar:', error);
    res.status(500).json({ 
      error: 'Error regenerating blockout calendar',
      message: error.message
    });
  }
});

// Admin calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/admin.ics', async (req, res) => {
  return res.redirect(301, '/admin/calendar?format=ics');
});

app.get('/calendar/admin', async (req, res) => {
  return res.redirect(301, '/admin/calendar?format=ics');
});

// Travel calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/travel.ics', async (req, res) => {
  return res.redirect(301, '/travel/calendar?format=ics');
});

app.get('/calendar/travel', async (req, res) => {
  return res.redirect(301, '/travel/calendar?format=ics');
});

// Blockout calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/blockout.ics', async (req, res) => {
  return res.redirect(301, '/blockout/calendar?format=ics');
});

app.get('/calendar/blockout', async (req, res) => {
  return res.redirect(301, '/blockout/calendar?format=ics');
});

// ICS calendar endpoint (with .ics extension) - serve calendar directly
app.get('/calendar/:personId.ics', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Remove .ics extension from personId
    personId = personId.replace(/\.ics$/, '');
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Redirect to main calendar endpoint with format=ics
    // This ensures we use the new Calendar Data database
    return res.redirect(301, `/calendar/${personId}?format=ics`);
  
  } catch (error) {
    console.error('ICS calendar generation error:', error);
    res.status(500).json({ error: 'Error generating calendar' });
  }
});

// Calendar endpoint using Calendar Data database only
app.get('/calendar/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    const format = req.query.format;
    
    // Auto-detect format from Accept header for calendar subscriptions
    const acceptHeader = req.headers.accept || '';
    const shouldReturnICS = format === 'ics' || 
                           acceptHeader.includes('text/calendar') || 
                           acceptHeader.includes('application/calendar');

    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Check Redis cache first (if enabled, unless ?fresh=true is specified)
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:${personId}:${shouldReturnICS ? 'ics' : 'json'}`;
    let calendarData = null; // Store fetched data to avoid duplicate fetches
    
    if (forceFresh && redis && cacheEnabled) {
      // Clear cache if forcing fresh data (clear BOTH ICS and JSON to be safe)
      const icsKey = `calendar:${personId}:ics`;
      const jsonKey = `calendar:${personId}:json`;
      
      const icsExists = await redis.exists(icsKey);
      const jsonExists = await redis.exists(jsonKey);
      
      const icsDeleted = await redis.del(icsKey);
      const jsonDeleted = await redis.del(jsonKey);
      
      const icsStillExists = await redis.exists(icsKey);
      const jsonStillExists = await redis.exists(jsonKey);
      
      console.log(`🗑️  Force fresh cache clearing for ${personId}:`);
      console.log(`   ICS cache - Before: ${icsExists ? 'EXISTS' : 'MISS'}, Deleted: ${icsDeleted}, After: ${icsStillExists ? 'STILL EXISTS ❌' : 'CLEARED ✅'}`);
      console.log(`   JSON cache - Before: ${jsonExists ? 'EXISTS' : 'MISS'}, Deleted: ${jsonDeleted}, After: ${jsonStillExists ? 'STILL EXISTS ❌' : 'CLEARED ✅'}`);
      
      if (icsStillExists || jsonStillExists) {
        console.error(`⚠️  WARNING: Cache was not fully cleared for ${personId} with ?fresh=true!`);
      }
    }
    
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          console.log(`✅ Cache HIT for ${personId} (${shouldReturnICS ? 'ICS' : 'JSON'})`);
          
          if (shouldReturnICS) {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="calendar.ics"');
            return res.send(cachedData);
          } else {
            return res.json(JSON.parse(cachedData));
          }
        }
        console.log(`❌ Cache MISS for ${personId} (${shouldReturnICS ? 'ICS' : 'JSON'})`);
        
        // Try a quick synchronous fetch with 30-second timeout
        // If Notion responds quickly, serve immediately. If slow, trigger background regeneration.
        console.log(`⏱️  Attempting quick fetch for ${personId} (30s timeout)...`);
        
        try {
          const quickTimeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Quick fetch timeout')), 30000);
          });
          
          // Check if Calendar Data database is configured
          if (!CALENDAR_DATA_DB) {
            throw new Error('Calendar Data database not configured');
          }
          
          calendarData = await Promise.race([
            getCalendarDataFromDatabase(personId),
            quickTimeoutPromise
          ]);
          
          if (!calendarData || !calendarData.events || calendarData.events.length === 0) {
            throw new Error('No events found');
          }
          
          // Quick fetch succeeded! Continue with normal processing flow below
          console.log(`✅ Quick fetch succeeded for ${personId}, processing and caching...`);
        } catch (quickError) {
          // Quick fetch failed or timed out - trigger background regeneration
          console.log(`⏱️  Quick fetch ${quickError.message === 'Quick fetch timeout' ? 'timed out' : 'failed'} for ${personId}, triggering background regeneration...`);
          regenerateCalendarForPerson(personId).catch(err => {
            console.error(`Background regeneration failed for ${personId}:`, err);
          });
          
          if (shouldReturnICS) {
            res.setHeader('Content-Type', 'text/calendar');
            res.status(503).send(`BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Calendar Feed//EN
BEGIN:VEVENT
DTSTART:19900101T000000Z
DTEND:19900101T000000Z
SUMMARY:Calendar is being regenerated
DESCRIPTION:Your calendar is being regenerated. Please try again in a few moments.
END:VEVENT
END:VCALENDAR`);
          } else {
            return res.status(503).json({
              error: 'Calendar cache is empty',
              message: 'Your calendar is being regenerated in the background. Please try again in a few moments.',
              retryAfter: 30
            });
          }
          return;
        }
        
        // If we get here, quick fetch succeeded - continue with normal processing
        console.log(`✅ Quick fetch succeeded for ${personId}, continuing with normal flow...`);
      } catch (cacheError) {
        console.error('Redis cache read error:', cacheError);
        // Continue without cache if Redis fails
      }
    }
    
    // Check if Calendar Data database is configured
    if (!CALENDAR_DATA_DB) {
      return res.status(500).json({ 
        error: 'Calendar Data database not configured',
        message: 'Please set CALENDAR_DATA_DATABASE_ID environment variable'
      });
    }
    
    // Only reach here if cache is disabled or forceFresh=true, or if quick fetch already succeeded
    // If we already have calendarData from quick fetch, skip this
    if (!calendarData) {
      try {
        // Set a timeout promise that rejects after 50 seconds (before Railway's 60s timeout and Notion's 60s limit)
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Notion API query timeout')), 50000);
        });
        
        calendarData = await Promise.race([
          getCalendarDataFromDatabase(personId),
          timeoutPromise
        ]);
    } catch (error) {
      if (error.message === 'Notion API query timeout') {
        console.error(`⏱️  Notion query timeout for ${personId}, triggering background regeneration...`);
        regenerateCalendarForPerson(personId).catch(err => {
          console.error(`Background regeneration failed for ${personId}:`, err);
        });
        
        if (shouldReturnICS) {
          res.setHeader('Content-Type', 'text/calendar');
          return res.status(503).send(`BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Calendar Feed//EN
BEGIN:VEVENT
DTSTART:19900101T000000Z
DTEND:19900101T000000Z
SUMMARY:Calendar generation in progress
DESCRIPTION:Calendar generation is taking longer than expected. Please try again in a few moments.
END:VEVENT
END:VCALENDAR`);
        } else {
          return res.status(503).json({
            error: 'Calendar generation timeout',
            message: 'Calendar generation is taking longer than expected. It is being regenerated in the background. Please try again in a few moments.',
            retryAfter: 30
          });
        }
      }
      throw error;
    }
    }
    
    if (!calendarData || !calendarData.events || calendarData.events.length === 0) {
      return res.status(404).json({ 
        error: 'No events found',
        message: 'No calendar data found in Calendar Data database for this person'
      });
    }
    
    const events = calendarData;
    
    // Get person name from Personnel database
    const person = await notion.pages.retrieve({ page_id: personId });
    const personName = person.properties?.['Full Name']?.formula?.string || 'Unknown';
    const firstName = person.properties?.['First Name']?.formula?.string || personName.split(' ')[0];
    
    console.log(`Using Calendar Data database for ${personName} (${calendarData.events.length} events)`);

    // Process events into calendar format (same logic as before)
    const allCalendarEvents = [];
    
    // Handle both array format and object with events property
    const eventsArray = Array.isArray(events) ? events : events.events || [];
    
    // Extract top-level arrays for new data source (if available)
    const topLevelFlights = events.flights || [];
    const topLevelRehearsals = events.rehearsals || [];
    const topLevelHotels = events.hotels || [];
    const topLevelTransport = events.ground_transport || [];
    
    const topLevelTeamCalendar = events.team_calendar || [];
    
    eventsArray.forEach(event => {
      // Add main event (using same logic as before)
      if (event.event_name && event.event_date) {
        let eventTimes = parseUnifiedDateTime(event.event_date);
        
        if (eventTimes) {
          // Build payroll info for description (put at TOP)
          let payrollInfo = '';
          
          // Use direct fields from Calendar Data database
          if (event.position || event.pay_total || event.assignments) {
            if (event.position) {
              payrollInfo += `Position: ${event.position}\n`;
            }
            if (event.assignments) {
              payrollInfo += `Assignments: ${event.assignments}\n`;
            }
            if (event.pay_total) {
              payrollInfo += `Pay: $${event.pay_total}\n`;
            }
            payrollInfo += '\n'; // Add spacing after position info
          }

          // Build calltime info (after payroll, before general info)
          let calltimeInfo = '';
          if (event.calltime) {
            calltimeInfo = `➡️ Call Time: ${formatCallTime(event.calltime)}\n\n`;
          }

          // Build gear checklist info (after calltime, before personnel)
          let gearChecklistInfo = '';
          if (event.gear_checklist && event.gear_checklist.trim()) {
            gearChecklistInfo = `🔧 Gear Checklist: ${event.gear_checklist}\n\n`;
          }

          // Build event personnel info (after gear checklist, before general info)
          let eventPersonnelInfo = '';
          if (event.event_personnel && event.event_personnel.trim()) {
            eventPersonnelInfo = `👥 Event Personnel:\n${event.event_personnel}\n\n`;
          }

          // Build Notion URL info (after personnel, before general info)
          let notionUrlInfo = '';
          if (event.notion_url && event.notion_url.trim()) {
            notionUrlInfo = `Notion Link: ${event.notion_url}\n\n`;
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + calltimeInfo + gearChecklistInfo + eventPersonnelInfo + notionUrlInfo + (event.general_info || ''),
            location: event.venue_address || event.venue || '',
            band: event.band || '',
            mainEvent: event.event_name
        });
        }
      }
      
      // Add flight events (same logic as before)
      if (event.flights && Array.isArray(event.flights)) {
        event.flights.forEach(flight => {
          // Departure flight
          if (flight.departure_time && flight.departure_name) {
            let departureTimes = parseUnifiedDateTime(flight.departure_time);
            if (!departureTimes) {
              // Fallback to old format
              departureTimes = {
                start: flight.departure_time,
                end: flight.departure_arrival_time || flight.departure_time
              };
            }

            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Return flight
          if (flight.return_time && flight.return_name) {
            let returnTimes = parseUnifiedDateTime(flight.return_time);
            if (!returnTimes) {
              // Fallback to old format
              returnTimes = {
                start: flight.return_time,
                end: flight.return_arrival_time || flight.return_time
              };
            }

            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Departure layover flight
          if (flight.departure_lo_time && flight.departure_lo_flightnumber) {
            let departureLoTimes = parseUnifiedDateTime(flight.departure_lo_time);
            if (!departureLoTimes) {
              departureLoTimes = {
                start: flight.departure_lo_time,
                end: flight.departure_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_departure_layover',
              title: `✈️ Layover: ${flight.departure_lo_from_airport || 'N/A'} → ${flight.departure_lo_to_airport || 'N/A'}`,
              start: departureLoTimes.start,
              end: departureLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_lo_flightnumber || 'N/A'}\nFrom: ${flight.departure_lo_from_airport || 'N/A'}\nTo: ${flight.departure_lo_to_airport || 'N/A'}`,
              location: flight.departure_lo_from_airport_address || flight.departure_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Return layover flight
          if (flight.return_lo_time && flight.return_lo_flightnumber) {
            let returnLoTimes = parseUnifiedDateTime(flight.return_lo_time);
            if (!returnLoTimes) {
              returnLoTimes = {
                start: flight.return_lo_time,
                end: flight.return_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_return_layover',
              title: `✈️ Layover: ${flight.return_lo_from_airport || 'N/A'} → ${flight.return_lo_to_airport || 'N/A'}`,
              start: returnLoTimes.start,
              end: returnLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_lo_flightnumber || 'N/A'}\nFrom: ${flight.return_lo_from_airport || 'N/A'}\nTo: ${flight.return_lo_to_airport || 'N/A'}`,
              location: flight.return_lo_from_airport_address || flight.return_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add rehearsal events (same logic as before)
      if (event.rehearsals && Array.isArray(event.rehearsals)) {
        event.rehearsals.forEach(rehearsal => {
          if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
            // Use the same parseUnifiedDateTime function as other event types
            let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);

            // Build location string
            let location = 'TBD';
            if (rehearsal.rehearsal_location && rehearsal.rehearsal_address) {
              location = `${rehearsal.rehearsal_location}, ${rehearsal.rehearsal_address}`;
            } else if (rehearsal.rehearsal_location) {
              location = rehearsal.rehearsal_location;
            } else if (rehearsal.rehearsal_address) {
              location = rehearsal.rehearsal_address;
            }

            // Build description with rehearsal info at the top
            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal - ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: event.event_name
            });
  }
});
      }

      // Add hotel events (same logic as before)
      if (event.hotels && Array.isArray(event.hotels)) {
        event.hotels.forEach(hotel => {
          // Try new dates_booked format first, then fallback to old check_in/check_out
          let hotelTimes = null;
          
          if (hotel.dates_booked) {
            hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
          } else if (hotel.check_in && hotel.check_out) {
            // Fallback to old format - try to parse with unified function
            try {
              // Try to parse as unified format first
              hotelTimes = parseUnifiedDateTime(hotel.check_in);
              if (!hotelTimes) {
                // If that fails, create dates and apply Pacific offset
                const startDate = new Date(hotel.check_in);
                const endDate = new Date(hotel.check_out);
                
                const isDST = isDSTDate(startDate);
                const offsetHours = isDST ? 7 : 8;
                
                startDate.setHours(startDate.getHours() + offsetHours);
                endDate.setHours(endDate.getHours() + offsetHours);
                
                hotelTimes = {
            start: startDate,
                  end: endDate
                };
              }
            } catch (e) {
              console.warn('Unable to parse hotel dates:', hotel.check_in, hotel.check_out);
              return;
            }
          }

          if (hotelTimes) {
            // Format names on reservation - each name on a separate line
            let namesFormatted = 'N/A';
            if (hotel.names_on_reservation) {
              const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
              if (names.length > 0) {
                namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
              }
            }

            allCalendarEvents.push({
              type: 'hotel',
              title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
              start: hotelTimes.start,
              end: hotelTimes.end,
              description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
              location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
              url: hotel.hotel_url || '',
              confirmation: hotel.confirmation || '',
              hotelName: hotel.hotel_name || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add ground transport events (same logic as before)
      if (event.ground_transport && Array.isArray(event.ground_transport)) {
        event.ground_transport.forEach(transport => {
          if (transport.start && transport.end) {
            let transportTimes = parseUnifiedDateTime(transport.start);
            if (!transportTimes) {
              // Fallback: treat as single time point
              transportTimes = {
                start: transport.start,
                end: transport.end || transport.start
              };
            }

            // For ground transport, make events 30 minutes long
            const startTime = new Date(transportTimes.start);
            const endTime = new Date(startTime.getTime() + 30 * 60 * 1000); // Add 30 minutes
            
            // Ground transport times are already processed by parseUnifiedDateTime
            // No additional offset needed - same logic as main events

            // Format title to replace PICKUP/DROPOFF/MEET UP with proper capitalization
            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');

            // Build description based on event type using new property structure
            let description = '';
            
            if (transport.description) {
              // Extract driver and passenger info from description
              const driverMatch = transport.description.match(/Driver:\s*([^\n]+)/);
              const passengerMatch = transport.description.match(/Passenger:\s*([^\n]+)/);
              
              // Add driver info for all event types (formatted like rehearsals)
              if (driverMatch) {
                // Split drivers by comma and format each on a new line like rehearsals
                const drivers = driverMatch[1].split(',').map(d => d.trim()).filter(d => d);
                if (drivers.length > 0) {
                  description += 'Drivers:\n';
                  drivers.forEach(driver => {
                    description += `• ${driver}\n`;
                  });
                  description += '\n';
                }
              }
              
              // Only add passenger list for meeting events (ground_transport_meeting)
              if (passengerMatch && transport.type === 'ground_transport_meeting') {
                // Split passengers by comma and format each on a new line
                const passengers = passengerMatch[1].split(',').map(p => p.trim()).filter(p => p);
                if (passengers.length > 0) {
                  description += 'Passengers:\n';
                  passengers.forEach(passenger => {
                    description += `• ${passenger}\n`;
                  });
                  description += '\n';
                }
              }
              
              // Extract event-specific info sections based on event type
              if (transport.type === 'ground_transport_meeting') {
                // For meeting events, look for "Meet Up Info" section
                const meetupInfoMatch = transport.description.match(/Meet Up Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (meetupInfoMatch) {
                  const meetupInfo = meetupInfoMatch[1].trim();
                  if (meetupInfo) {
                    description += 'Meet Up Info:\n';
                    // Format meetup info with bullet points for each line
                    const meetupInfoLines = meetupInfo.split('\n').filter(line => line.trim());
                    meetupInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) {
                        description += `• ${trimmedLine}\n`;
                      }
                    });
                    description += '\n';
                  }
                }
              } else if (transport.type === 'ground_transport_pickup') {
                // For pickup events, look for "Pick Up Info" section
                const pickupInfoMatch = transport.description.match(/Pick Up Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (pickupInfoMatch) {
                  const pickupInfo = pickupInfoMatch[1].trim();
                  if (pickupInfo) {
                    description += 'Pick Up Info:\n';
                    // Format pickup info with bullet points for each line
                    const pickupInfoLines = pickupInfo.split('\n').filter(line => line.trim());
                    pickupInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) {
                        description += `• ${trimmedLine}\n`;
                      }
                    });
                    description += '\n';
                  }
                }
              } else if (transport.type === 'ground_transport_dropoff') {
                // For dropoff events, look for "Drop Off Info" section
                const dropoffInfoMatch = transport.description.match(/Drop Off Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
                if (dropoffInfoMatch) {
                  const dropoffInfo = dropoffInfoMatch[1].trim();
                  if (dropoffInfo) {
                    description += 'Drop Off Info:\n';
                    // Format dropoff info with bullet points for each line
                    const dropoffInfoLines = dropoffInfo.split('\n').filter(line => line.trim());
                    dropoffInfoLines.forEach(line => {
                      const trimmedLine = line.trim();
                      if (trimmedLine) {
                        description += `• ${trimmedLine}\n`;
                      }
                    });
                    description += '\n';
                  }
                }
              }
              
              // Add confirmation info if present
              const confirmationMatch = transport.description.match(/Confirmation:\s*([^\n]+)/);
              if (confirmationMatch) {
                description += `Confirmation: ${confirmationMatch[1]}\n`;
              }
              
              // Add transportation URL if present
              if (transport.transportation_url) {
                description += `\nNotion Link: ${transport.transportation_url}`;
              }
    } else {
              description = 'Ground transportation details';
            }

            allCalendarEvents.push({
              type: transport.type || 'ground_transport',
              title: `🚙 ${formattedTitle}`,
              start: startTime.toISOString(),
              end: endTime.toISOString(),
              description: description.trim(),
              location: transport.location || '',
              mainEvent: event.event_name
            });

            // Note: Meetup events are now handled directly by the Notion formula as ground_transport_meeting events
            // No need to parse Passenger Info for meetup events anymore
          }
        });
      }
    });
    
    // Process top-level arrays (for new data source)
    // These arrays are shared across all events and should only be processed once
    if (topLevelFlights.length > 0) {
      topLevelFlights.forEach(flight => {
        // Departure flight
        if (flight.departure_time && flight.departure_name) {
          let departureTimes = parseUnifiedDateTime(flight.departure_time);
          if (departureTimes) {
            // Build description
            let description = `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: description,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: '' // Top-level flights aren't tied to a specific event
            });
          }
        }

        // Return flight
        if (flight.return_time && flight.return_name) {
          let returnTimes = parseUnifiedDateTime(flight.return_time);
          if (returnTimes) {
            // Build description
            let description = `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: description,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: '' // Top-level flights aren't tied to a specific event
            });
          }
        }
      });
    }

    if (topLevelRehearsals.length > 0) {
      topLevelRehearsals.forEach(rehearsal => {
        if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
          // Use the same parseUnifiedDateTime function as other event types
          let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
          
          if (rehearsalTimes) {
            // Use rehearsal_address for location (clean up invisible characters)
            let location = rehearsal.rehearsal_address ? rehearsal.rehearsal_address.trim().replace(/\u2060/g, '') : 'TBD';

            let description = `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: '' // Top-level rehearsals aren't tied to a specific event
            });
          }
        }
      });
    }

    if (topLevelHotels.length > 0) {
      topLevelHotels.forEach(hotel => {
        let hotelTimes = null;
        
        if (hotel.dates_booked) {
          hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
        }

        if (hotelTimes) {
          let namesFormatted = 'N/A';
          if (hotel.names_on_reservation) {
            const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
            if (names.length > 0) {
              namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
            }
          }

          allCalendarEvents.push({
            type: 'hotel',
            title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
            start: hotelTimes.start,
            end: hotelTimes.end,
            description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
            location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
            url: hotel.hotel_url || '',
            confirmation: hotel.confirmation || '',
            hotelName: hotel.hotel_name || '',
            mainEvent: '' // Top-level hotels aren't tied to a specific event
          });
        }
      });
    }

    if (topLevelTransport.length > 0) {
      topLevelTransport.forEach(transport => {
        if (transport.start) {
          let transportTimes = parseUnifiedDateTime(transport.start);
          if (transportTimes) {
            const startTime = new Date(transportTimes.start);
            const endTime = new Date(startTime.getTime() + 30 * 60 * 1000);

            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');

            let description = '';
            if (transport.description) {
              const driverMatch = transport.description.match(/Driver:\s*([^\n]+)/);
              
              if (driverMatch) {
                const drivers = driverMatch[1].split(',').map(d => d.trim()).filter(d => d);
                if (drivers.length > 0) {
                  description += 'Drivers:\n';
                  drivers.forEach(driver => {
                    description += `• ${driver}\n`;
                  });
                  description += '\n';
                }
              }
              
              description += transport.description.replace(/Driver:\s*[^\n]+\n?/, '');
            }
            
            // Add transportation URL if present
            if (transport.transportation_url) {
              description += `\n\nNotion Link: ${transport.transportation_url}`;
            }

            let eventType = 'ground_transport';
            if (transport.type === 'ground_transport_pickup') {
              eventType = 'ground_transport_pickup';
            } else if (transport.type === 'ground_transport_dropoff') {
              eventType = 'ground_transport_dropoff';
            } else if (transport.type === 'ground_transport_meeting') {
              eventType = 'ground_transport_meeting';
            }

            allCalendarEvents.push({
              type: eventType,
              title: `🚙 ${formattedTitle}`,
              start: startTime,
              end: endTime,
              description: description,
              location: transport.location || '',
              mainEvent: '' // Top-level transport isn't tied to a specific event
            });
          }
        }
      });
    }

    if (topLevelTeamCalendar.length > 0) {
      topLevelTeamCalendar.forEach(teamEvent => {
        if (teamEvent.date) {
          let eventTimes = parseUnifiedDateTime(teamEvent.date);
          if (eventTimes) {
            // Use ⛔️ emoji for OOO events, 💼 for Meeting events, 📅 for Office and other events
            const isOOO = teamEvent.title && teamEvent.title.trim().toUpperCase() === 'OOO';
            const isMeeting = teamEvent.title && teamEvent.title.trim().toUpperCase().includes('MEETING');
            let emoji;
            if (isOOO) {
              emoji = '⛔️';
            } else if (isMeeting) {
              emoji = '💼';
            } else {
              emoji = '📅';
            }
            
            // For OOO events, add one day to end date to make it inclusive
            // In iCal format, end date is exclusive, so we need Dec 17 to block through Dec 16
            let endDate = eventTimes.end;
            if (isOOO) {
              endDate = new Date(eventTimes.end);
              endDate.setDate(endDate.getDate() + 1);
            }
            
            allCalendarEvents.push({
              type: 'team_calendar',
              title: `${emoji} ${teamEvent.title || 'Team Event'}`,
              start: eventTimes.start,
              end: endDate,
              description: [teamEvent.dcos, teamEvent.notes].filter(Boolean).join('\n\n'),
              location: teamEvent.address || '',
              url: teamEvent.notion_link || '',
              mainEvent: '' // Top-level team calendar events aren't tied to a specific event
            });
          }
        }
      });
    }

    
    if (shouldReturnICS) {
      // Generate ICS calendar with all events
      const calendar = ical({ 
        name: `Downbeat iCal (${firstName})`,
        description: `Professional events calendar for ${personName}`,
        ttl: 300  // Suggest refresh every 5 minutes
      });

      allCalendarEvents.forEach(event => {
        // event.start and event.end are already Date objects for new format
        // or strings for old format
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
          
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,  // Create floating timezone events - times stay the same regardless of timezone
          alarms: getAlarmsForEvent(event.type, event.title)  // Add event-specific alarms
        });
      });

      const icsData = calendar.toString();
      
      // Cache the ICS data (if enabled)
      if (redis && cacheEnabled) {
        try {
          await redis.setEx(cacheKey, CACHE_TTL, icsData);
          console.log(`💾 Cached ICS for ${personId} (TTL: ${CACHE_TTL}s)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }

      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="calendar.ics"');
      return res.send(icsData);
    }

    // Return JSON format with expanded events
    const jsonResponse = {
      personName: personName,
      totalMainEvents: eventsArray.length,
      totalCalendarEvents: allCalendarEvents.length,
      dataSource: 'calendar_data_database',
      breakdown: {
        mainEvents: allCalendarEvents.filter(e => e.type === 'main_event').length,
        flights: allCalendarEvents.filter(e => e.type === 'flight_departure' || e.type === 'flight_return' || e.type === 'flight_departure_layover' || e.type === 'flight_return_layover').length,
        rehearsals: allCalendarEvents.filter(e => e.type === 'rehearsal').length,
        hotels: allCalendarEvents.filter(e => e.type === 'hotel').length,
        groundTransport: allCalendarEvents.filter(e => e.type === 'ground_transport_pickup' || e.type === 'ground_transport_dropoff' || e.type === 'ground_transport_meeting' || e.type === 'ground_transport').length
      },
      events: allCalendarEvents
    };

    // Cache the JSON data (if enabled)
    if (redis && cacheEnabled) {
      try {
        await redis.setEx(cacheKey, CACHE_TTL, JSON.stringify(jsonResponse));
        console.log(`💾 Cached JSON for ${personId} (TTL: ${CACHE_TTL}s)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    res.json(jsonResponse);
    
  } catch (error) {
    console.error('Calendar generation error:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({ 
      error: 'Error generating calendar',
      message: error.message,
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Flight countdown page route - serves modern design
app.get('/flight/:flightId', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'public', 'flight-countdown-modern.html'));
});

// Fallback route for URL parameters (if modern design needs it)
app.get('/flight-countdown-modern.html', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'public', 'flight-countdown-modern.html'));
});

// Start background job for calendar updates
startBackgroundJob();

app.listen(port, () => {
  console.log(`Calendar feed server running on port ${port}`);
  console.log(`Background job active - updating all people every 5 minutes (paginated, batched parallel)`);
});
