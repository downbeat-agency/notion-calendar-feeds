import 'dotenv/config';
import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';
import { createClient } from 'redis';

// Server refresh - October 1, 2025

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

// Serve static files from public directory
app.use(express.static('public'));

// Use environment variable for Personnel database ID
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;

// Cache TTL in seconds (8 minutes for 5-minute background refresh cycle)
const CACHE_TTL = parseInt(process.env.CACHE_TTL) || 480;

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
                const utcDate = new Date(event.calltime);
                const laDate = new Date(utcDate.toLocaleString("en-US", {timeZone: "America/Los_Angeles"}));
                const hours = laDate.getHours();
                const minutes = laDate.getMinutes().toString().padStart(2, '0');
                let displayHours = hours;
                let period = 'AM';
                if (hours === 0) {
                  displayHours = 12;
                } else if (hours === 12) {
                  period = 'PM';
                } else if (hours > 12) {
                  displayHours = hours - 12;
                  period = 'PM';
                }
                displayCalltime = `${displayHours}:${minutes} ${period}`;
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

          let notionUrlInfo = '';
          if (event.notion_url && event.notion_url.trim()) {
            notionUrlInfo = `Notion Link: ${event.notion_url}\n\n`;
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + calltimeInfo + gearChecklistInfo + notionUrlInfo + (event.general_info || ''),
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
            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.departure_airport || '',
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
            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.return_airport || '',
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

            let description = `Rehearsal for ${event.event_name}`;
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
            
            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: description,
              location: flight.departure_airport || '',
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
            
            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: description,
              location: flight.return_airport || '',
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
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000; 
            color: #fff; 
            min-height: 100vh;
        }
        .container { 
            max-width: 600px; 
            margin: 0 auto; 
            background: #111; 
            padding: 50px; 
            border-radius: 8px; 
            box-shadow: 0 20px 40px rgba(0,0,0,0.5);
            border: 1px solid #333;
        }
        h1 { 
            color: #fff; 
            margin-bottom: 15px; 
            font-size: 2.5rem; 
            font-weight: 300;
            letter-spacing: 2px;
            text-align: center;
        }
        .subtitle {
            color: #888;
            font-size: 1.1rem;
            text-align: center;
            margin-bottom: 30px;
            font-weight: 300;
        }
        .url-box { 
            background: #1a1a1a; 
            padding: 20px; 
            border-radius: 6px; 
            border: 1px solid #333; 
            margin: 25px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #ccc;
            font-size: 14px;
        }
        .copy-btn { 
            background: #000; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 15px 30px; 
            border-radius: 6px; 
            cursor: pointer; 
            margin: 15px 0; 
            font-size: 16px;
            transition: all 0.3s ease;
        }
        .copy-btn:hover { 
            background: #333; 
            border-color: #555;
        }
        .instructions { 
            background: #1a1a1a; 
            padding: 25px; 
            border-radius: 6px; 
            border-left: 3px solid #333; 
            margin: 30px 0; 
            color: #ccc;
        }
        .instructions strong {
            color: #fff;
        }
        .app-links { 
            display: flex; 
            gap: 15px; 
            margin: 30px 0; 
        }
        .app-link { 
            flex: 1; 
            padding: 20px; 
            text-align: center; 
            background: #000; 
            color: #fff; 
            text-decoration: none; 
            border-radius: 6px; 
            border: 1px solid #333;
            transition: all 0.3s ease;
            font-weight: 500;
        }
        .app-link:hover { 
            background: #1a1a1a; 
            border-color: #555;
        }
        .section-title {
            color: #fff;
            font-size: 1.1rem;
            margin-bottom: 15px;
            font-weight: 500;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>${personName ? `Hello ${personName}` : 'Subscribe to Downbeat Calendar'}</h1>
        ${personName ? '<div class="subtitle">Subscribe to Downbeat Calendar</div>' : ''}
        
        <div class="instructions">
            <div class="section-title">Quick Subscribe</div>
            Click one of the options below to automatically open your calendar app with this calendar pre-loaded.
        </div>
        
        <div class="app-links">
            <a href="webcal://${req.get('host')}/calendar/${personId}" class="app-link">
                🍎 Apple Calendar
            </a>
        </div>
        
        <div class="instructions" style="background: #1a1a1a; border-left-color: #ffa500;">
            <div class="section-title">📝 Google Calendar Note</div>
            <strong>Select "From URL" in the left menu, then paste the URL below to add the calendar.</strong>
        </div>
        
        <div class="app-links">
            <a href="https://calendar.google.com/calendar/u/0/r/settings/addcalendar" class="app-link" target="_blank">
                📅 Google Calendar
            </a>
        </div>
        
        <div class="instructions">
            <div class="section-title">Manual Setup</div>
            <p>Copy this URL to add the calendar manually:</p>
            <div class="url-box" id="urlBox">${subscriptionUrl}</div>
            <button class="copy-btn" onclick="copyUrl()">📋 Copy URL</button>
        </div>
        
        <div class="instructions">
            <div class="section-title">Setup Instructions</div>
            <strong>Apple Calendar:</strong> Just press the Apple Calendar button.<br><br>
            <strong>Google Calendar:</strong> Click the Google Calendar button above → Click "From URL" in the left menu → Paste the URL below in the "URL of the calendar" field → Click "Add calendar"<br><br>
            <strong>Outlook:</strong> Copy URL → Calendar → Add calendar → Subscribe from web → Paste URL
        </div>
    </div>
    
    <script>
        function copyUrl() {
            const urlBox = document.getElementById('urlBox');
            navigator.clipboard.writeText(urlBox.textContent).then(() => {
                const btn = document.querySelector('.copy-btn');
                const originalText = btn.textContent;
                btn.textContent = '✅ Copied!';
                setTimeout(() => btn.textContent = originalText, 2000);
            });
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
                // Parse the UTC timestamp
                const utcDate = new Date(event.calltime);
                
                // Convert to America/Los_Angeles timezone
                const laDate = new Date(utcDate.toLocaleString("en-US", {timeZone: "America/Los_Angeles"}));
                
                // Format as floating time (no timezone info)
                const hours = laDate.getHours();
                const minutes = laDate.getMinutes().toString().padStart(2, '0');
                
                // Convert to 12-hour format
                let displayHours = hours;
                let period = 'AM';
                
                if (hours === 0) {
                  displayHours = 12;
                } else if (hours === 12) {
                  period = 'PM';
                } else if (hours > 12) {
                  displayHours = hours - 12;
                  period = 'PM';
                }
                
                displayCalltime = `${displayHours}:${minutes} ${period}`;
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

          // Build gear checklist info (after calltime, before general info)
          let gearChecklistInfo = '';
          if (event.gear_checklist && event.gear_checklist.trim()) {
            gearChecklistInfo = `🔧 Gear Checklist: ${event.gear_checklist}\n\n`;
          }

          // Build Notion URL info (after gear checklist, before general info)
          let notionUrlInfo = '';
          if (event.notion_url && event.notion_url.trim()) {
            notionUrlInfo = `Notion Link: ${event.notion_url}\n\n`;
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + calltimeInfo + gearChecklistInfo + notionUrlInfo + (event.general_info || ''),
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

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.departure_airport || '',
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

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.return_airport || '',
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

            // Build description with band personnel
            let description = `Rehearsal for ${event.event_name}`;
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
            
            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: description,
              location: flight.departure_airport || '',
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
            
            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: description,
              location: flight.return_airport || '',
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

// Start background job for calendar updates
startBackgroundJob();

app.listen(port, () => {
  console.log(`Calendar feed server running on port ${port}`);
  console.log(`Background job active - updating all people every 5 minutes (batched parallel)`);
});
