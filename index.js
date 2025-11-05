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
// DEBUG VERSION - Testing deployment process

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ auth: process.env.NOTION_API_KEY });

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

// Helper function to get calendar data from Calendar Data database
async function getCalendarDataFromDatabase(personId) {
  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
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

  if (response.results.length === 0) {
    return null;
  }

  const calendarData = response.results[0].properties;
  
  // Parse all the JSON strings with better error handling
  let events, flights, transportation, hotels, rehearsals, teamCalendar;
  
  try {
    events = JSON.parse(calendarData.Events?.formula?.string || '[]');
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
          return {
            start: startDate,
            end: endDate
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
      const [startStr, endStr] = cleanStr.split('/');
      const startDate = new Date(startStr.trim());
      const endDate = new Date(endStr.trim());
      
      if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
        // Check if these are UTC timestamps and convert to Pacific floating time
        const isUTCStart = startStr.includes('T') && (startStr.includes('Z') || startStr.includes('+00:00'));
        const isUTCEnd = endStr.includes('T') && (endStr.includes('Z') || endStr.includes('+00:00'));
        
        if (isUTCStart) {
          const isDST = isDSTDate(startDate);
          const offsetHours = isDST ? 7 : 8;
          startDate.setHours(startDate.getHours() - offsetHours);
        }
        
        if (isUTCEnd) {
          const isDST = isDSTDate(endDate);
          const offsetHours = isDST ? 7 : 8;
          endDate.setHours(endDate.getHours() - offsetHours);
        }
        
        return {
          start: startDate,
          end: endDate
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
        // Subtract Pacific offset to convert UTC to Pacific floating time
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
    
    // Get calendar data from Calendar Data database
    const calendarData = await getCalendarDataFromDatabase(personId);
    if (!calendarData || !calendarData.events || calendarData.events.length === 0) {
      console.log(`⚠️  No events found for ${personId}, skipping...`);
      return { success: false, personId, reason: 'no_events' };
    }
    
    const events = calendarData;
    
    // Get person name from Personnel database
    const person = await notion.pages.retrieve({ page_id: personId });
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
        let eventTimes = parseUnifiedDateTime(event.event_date);
        
        if (eventTimes) {
          let payrollInfo = '';
          if (event.position || event.pay_total || event.assignments) {
            if (event.position) payrollInfo += `Position: ${event.position}\n`;
            if (event.assignments) payrollInfo += `Assignments: ${event.assignments}\n`;
            if (event.pay_total) payrollInfo += `Pay: $${event.pay_total}\n`;
            payrollInfo += '\n';
          }

          let calltimeInfo = '';
          if (event.calltime && event.calltime.trim()) {
            let displayCalltime = event.calltime;
            
            if (event.calltime.includes('T') && (event.calltime.includes('Z') || event.calltime.includes('+00:00'))) {
              try {
                // Parse UTC timestamp
                const utcDate = new Date(event.calltime);
                
                // Convert UTC to America/Los_Angeles timezone and display as floating time
                // Floating time means: display the Pacific time components directly without
                // further timezone conversion (as if UTC = Pacific time)
                const formatter = new Intl.DateTimeFormat('en-US', {
                  timeZone: 'America/Los_Angeles',
                  hour: 'numeric',
                  minute: '2-digit',
                  hour12: true
                });
                
                displayCalltime = formatter.format(utcDate);
              } catch (e) {
                console.warn('Failed to parse calltime:', event.calltime, e);
              }
            }
            
            calltimeInfo = `➡️ Call Time: ${displayCalltime}\n\n`;
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
              location: flight.departure_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL
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
              location: flight.return_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
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
              url: rehearsal.rehearsal_pco || '',
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
            if (flight.flight_url) description += `\n\nNotion Link: ${flight.flight_url}`;
            
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
              location: flight.departure_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
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
            if (flight.flight_url) description += `\n\nNotion Link: ${flight.flight_url}`;
            
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
              location: flight.return_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
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
              url: rehearsal.rehearsal_pco || '',
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
            const emoji = isOOO ? '⛔️' : '📅';
            
            allCalendarEvents.push({
              type: 'team_calendar',
              title: `${emoji} ${teamEvent.title || 'Team Event'}`,
              start: eventTimes.start,
              end: eventTimes.end,
              description: teamEvent.notes || '',
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

// Background job to update 100 people in parallel batches every 5 minutes
function startBackgroundJob() {
  console.log('🔄 Starting background calendar refresh job (every 5 minutes)');
  console.log('   Processing 100 people in parallel each cycle');
  
  setInterval(async () => {
    try {
      const jobStart = Date.now();
      console.log('\n⏰ Background job triggered - updating 100 people in parallel...');
      
      // Get all person IDs from Personnel database
      const response = await notion.databases.query({
        database_id: PERSONNEL_DB,
        page_size: 100
      });

      const personIds = response.results.map(page => page.id);
      
      if (personIds.length === 0) {
        console.log('⚠️  No personnel found in database');
        return;
      }
      
      console.log(`   Found ${personIds.length} people to update`);
      
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
      
      const jobTime = Math.round((Date.now() - jobStart) / 1000);
      console.log(`✅ Background refresh complete in ${jobTime}s: ${totalSuccess} success, ${totalFailed} failed, ${totalSkipped} skipped`);
      
    } catch (error) {
      console.error('❌ Background job error:', error.message);
    }
  }, 5 * 60 * 1000); // 5 minutes
}

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
      interval: '30 minutes',
      description: 'Updates one random person every 30 minutes'
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
    
    // Count actual events from all JSON arrays
    let totalActualEvents = 0;
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
      } catch (e) {
        console.warn('Error parsing JSON in debug endpoint:', e);
      }
    });
    
    res.json({
      personId: personId,
      totalDatabaseRows: response.results.length,
      totalActualEvents: totalActualEvents,
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

    // Check Redis cache first (if enabled)
    const cacheKey = `calendar:${personId}:${shouldReturnICS ? 'ics' : 'json'}`;
    if (redis && cacheEnabled) {
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
        console.log(`❌ Cache MISS for ${personId} (${shouldReturnICS ? 'ICS' : 'JSON'}) - fetching from Notion...`);
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
    
    // Get calendar data from Calendar Data database
    const calendarData = await getCalendarDataFromDatabase(personId);
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
          if (event.calltime && event.calltime.trim()) {
            let displayCalltime = event.calltime;
            
            // Check if calltime is an ISO timestamp (UTC)
            if (event.calltime.includes('T') && (event.calltime.includes('Z') || event.calltime.includes('+00:00'))) {
              try {
                // Parse UTC timestamp
                const utcDate = new Date(event.calltime);
                
                // Convert UTC to America/Los_Angeles timezone and display as floating time
                // Floating time means: display the Pacific time components directly without
                // further timezone conversion (as if UTC = Pacific time)
                const formatter = new Intl.DateTimeFormat('en-US', {
                  timeZone: 'America/Los_Angeles',
                  hour: 'numeric',
                  minute: '2-digit',
                  hour12: true
                });
                
                displayCalltime = formatter.format(utcDate);
              } catch (e) {
                console.warn('Failed to parse calltime ISO timestamp:', event.calltime, e);
                // Fall back to original value
              }
            } else {
              // Try to parse the time and adjust for timezone (legacy format)
              const timeMatch = event.calltime.match(/(\d{1,2}):(\d{2})\s*(AM|PM)/i);
              if (timeMatch) {
                let hours = parseInt(timeMatch[1]);
                const minutes = timeMatch[2];
                const period = timeMatch[3].toUpperCase();
                
                // Convert to 24-hour format
                if (period === 'PM' && hours !== 12) hours += 12;
                if (period === 'AM' && hours === 12) hours = 0;
                
                // Subtract 7 hours to convert from UTC back to Pacific (PDT)
                // Use 7 for PDT (most of the year), 8 for PST (winter)
                hours -= 7;
                
                // Handle negative hours (wrap to previous day)
                if (hours < 0) hours += 24;
                if (hours >= 24) hours -= 24;
                
                // Convert back to 12-hour format
                let newPeriod = 'AM';
                let displayHours = hours;
                if (hours === 0) {
                  displayHours = 12;
                } else if (hours === 12) {
                  newPeriod = 'PM';
                } else if (hours > 12) {
                  displayHours = hours - 12;
                  newPeriod = 'PM';
                }
                
                displayCalltime = `${displayHours}:${minutes} ${newPeriod}`;
              }
            }
            
            calltimeInfo = `➡️ Call Time: ${displayCalltime}\n\n`;
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
              location: flight.departure_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
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
              location: flight.return_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
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
              url: rehearsal.rehearsal_pco || '',
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
            // Build description with optional Notion link
            let description = `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`;
            if (flight.flight_url) {
              description += `\n\nNotion Link: ${flight.flight_url}`;
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
              description: description,
              location: flight.departure_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
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
            // Build description with optional Notion link
            let description = `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`;
            if (flight.flight_url) {
              description += `\n\nNotion Link: ${flight.flight_url}`;
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
              description: description,
              location: flight.return_airport || '',
              url: countdownUrl, // Always use countdown URL as the main event URL // Always use countdown URL as the main event URL
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
              url: rehearsal.rehearsal_pco || '',
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
            // Use ⛔️ emoji for OOO events, 📅 for regular events
            const isOOO = teamEvent.title && teamEvent.title.trim().toUpperCase() === 'OOO';
            const emoji = isOOO ? '⛔️' : '📅';
            
            allCalendarEvents.push({
              type: 'team_calendar',
              title: `${emoji} ${teamEvent.title || 'Team Event'}`,
              start: eventTimes.start,
              end: eventTimes.end,
              description: teamEvent.notes || '',
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
        flights: allCalendarEvents.filter(e => e.type === 'flight_departure' || e.type === 'flight_return').length,
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
  console.log(`Background job active - updating all people every 5 minutes (batched parallel)`);
});
