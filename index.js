import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ auth: process.env.NOTION_API_KEY });

// Use environment variable for Personnel database ID
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;

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

// Helper function to parse @ format dates (for flights, rehearsals, hotels, transport)
function parseUnifiedDateTime(dateTimeStr) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  // Clean up the string
  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  
  // Check if it's the unified format with @
  if (cleanStr.startsWith('@')) {
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
        // Create UTC dates that represent the correct Pacific times
        // We need to create dates that, when interpreted as local time, show the correct Pacific times
        
        // Parse the date and time components manually
        const startDateObj = new Date(`${dateStr} ${startTimeStr}`);
        const endDateObj = new Date(`${endDateStr} ${endTimeStr}`);
        
        // Create UTC dates by manually constructing them
        // For Pacific time, we need to create UTC dates that represent the Pacific times
        const startYear = startDateObj.getFullYear();
        const startMonth = startDateObj.getMonth();
        const startDay = startDateObj.getDate();
        const startHour = startDateObj.getHours();
        const startMinute = startDateObj.getMinutes();
        
        const endYear = endDateObj.getFullYear();
        const endMonth = endDateObj.getMonth();
        const endDay = endDateObj.getDate();
        const endHour = endDateObj.getHours();
        const endMinute = endDateObj.getMinutes();
        
        // Create UTC dates that, when interpreted as local time, show the correct Pacific times
        // We need to add the timezone offset to get UTC times that represent Pacific times
        const isPDT = startMonth >= 2 && startMonth <= 10; // March to November
        const offsetHours = isPDT ? 7 : 8; // PDT is UTC-7, PST is UTC-8
        
        // Convert to UTC normally first
        const startDate = new Date(Date.UTC(startYear, startMonth, startDay, startHour + offsetHours, startMinute));
        const endDate = new Date(Date.UTC(endYear, endMonth, endDay, endHour + offsetHours, endMinute));
        
        // If the original START time was 5 PM or later, subtract 24 hours to keep it on the same day
        // This prevents events from shifting to the next day when converted to UTC
        if (startHour >= 17) { // Start time is 5 PM or later
          startDate.setUTCHours(startDate.getUTCHours() - 24);
          endDate.setUTCHours(endDate.getUTCHours() - 24);
        }
        
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
        const tempDate = new Date(dateStr);
        const offset = getPacificOffset(tempDate);
        const date = new Date(`${dateStr}${offset}`);
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
  
  // Fallback: try to parse as regular ISO date
  try {
    const date = new Date(cleanStr);
    if (!isNaN(date.getTime())) {
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

// Health check endpoint
app.get('/', (_req, res) => {
  res.json({
    status: 'Calendar Feed Server Running (Updated)',
    endpoints: {
      subscribe: '/subscribe/:personId',
      calendar: '/calendar/:personId',
      ics: '/calendar/:personId?format=ics',
      debug: '/debug/simple-test/:personId'
    }
  });
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

    // No need to fetch person data - just use generic title for speed
    const personName = 'Downbeat Calendar';
    const subscriptionUrl = `${req.protocol}://${req.get('host')}/calendar/${personId}`;
    
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
    <title>Subscribe to ${personName}'s Calendar</title>
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
            margin-bottom: 30px; 
            font-size: 2.5rem; 
            font-weight: 300;
            letter-spacing: 2px;
            text-align: center;
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
        <h1>Subscribe to Downbeat Calendar</h1>
        
        <div class="instructions">
            <div class="section-title">Quick Subscribe</div>
            Click one of the options below to automatically open your calendar app with this calendar pre-loaded.
        </div>
        
        <div class="app-links">
            <a href="webcal://${req.get('host')}/calendar/${personId}" class="app-link">📱 Mobile Calendar</a>
            <a href="https://calendar.google.com/calendar/render?cid=${encodeURIComponent(subscriptionUrl)}" class="app-link">📅 Google Calendar</a>
        </div>
        
        <div class="section-title">Manual Setup</div>
        <p>Copy this URL to add the calendar manually:</p>
        <div class="url-box" id="urlBox">${subscriptionUrl}</div>
        <button class="copy-btn" onclick="copyUrl()">📋 Copy URL</button>
        
        <div class="instructions">
            <div class="section-title">Setup Instructions</div>
            <strong>iPhone/Apple Calendar:</strong> Copy URL → Calendar app → File → New Calendar Subscription → Paste URL<br><br>
            <strong>Android/Google Calendar:</strong> Copy URL → Google Calendar → Settings → Add calendar → From URL → Paste URL<br><br>
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

// Main calendar endpoint
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

    // Get person from Personnel database
    const person = await notion.pages.retrieve({ page_id: personId });
    
    if (!person) {
      return res.status(404).json({ error: 'Person not found' });
    }

    // Get Calendar Feed JSON from person's formula property
    const calendarFeedJson = person.properties?.['Calendar Feed JSON']?.formula?.string;
    
    // Also get Hotels JSON if it exists (for testing)
    const hotelsJson = person.properties?.['Hotels JSON']?.formula?.string;
    
    // Debug logging removed for performance
    
    if (!calendarFeedJson) {
      return res.status(404).json({ error: 'No calendar feed data found' });
    }

    // Parse the JSON data
    let calendarData;
    try {
      calendarData = JSON.parse(calendarFeedJson);
    } catch (parseError) {
      return res.status(500).json({ error: 'Invalid calendar feed JSON' });
    }

    // Extract events array
    const events = Array.isArray(calendarData) ? calendarData : calendarData.events || [];

    // Parse separate Hotels JSON if it exists
    let hotelsData = null;
    if (hotelsJson) {
      try {
        hotelsData = JSON.parse(hotelsJson);
        console.log('Parsed Hotels JSON:', hotelsData);
      } catch (e) {
        console.warn('Failed to parse Hotels JSON:', e.message);
      }
    }

    // Process all events into a flat array including main events, flights, and rehearsals
    const allCalendarEvents = [];
    
    events.forEach(event => {
      // Add main event (using same logic as rehearsals)
      if (event.event_name && event.event_date) {
        // Parse event date/time using the same logic as rehearsal_time
        let eventTimes = parseUnifiedDateTime(event.event_date);
        
        if (eventTimes) {
          // Build payroll info for description (put at TOP)
          let payrollInfo = '';
          if (event.payroll && Array.isArray(event.payroll) && event.payroll.length > 0) {
            event.payroll.forEach(payroll => {
              payrollInfo += `Position: ${payroll.position || 'N/A'}\n`;
              if (payroll.assignment) {
                payrollInfo += `Assignment: ${payroll.assignment}\n`;
              }
              if (payroll.pay_total) {
                payrollInfo += `Pay: $${payroll.pay_total}\n`;
              }
            });
            payrollInfo += '\n'; // Add spacing after position info
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + (event.general_info || ''),
            location: event.venue_address || event.venue || '',
            url: event.notion_url || '',
            band: event.band || '',
            mainEvent: event.event_name
        });
        }
      }
      
      // Add flight events
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
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nAirline: ${flight.departure_airline || 'N/A'}\nFlight: ${flight.departure_flightnumber || 'N/A'}`,
              location: flight.departure_from || 'Airport',
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
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nAirline: ${flight.return_airline || 'N/A'}\nFlight: ${flight.return_flightnumber || 'N/A'}`,
              location: flight.return_from || 'Airport',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
    });
  }
});
      }

      // Add rehearsal events
      if (event.rehearsals && Array.isArray(event.rehearsals)) {
        event.rehearsals.forEach(rehearsal => {
          if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
            let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
            if (!rehearsalTimes) {
              // Fallback: treat as single time point
              rehearsalTimes = {
                start: rehearsal.rehearsal_time,
                end: rehearsal.rehearsal_time
              };
            }

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
              mainEvent: event.event_name
            });
  }
});
      }

      // Add hotel events (from event hotels or separate Hotels JSON property)
      let hotelsToProcess = event.hotels || [];
      
      // If we have separate Hotels JSON data, merge it in for this event
      if (hotelsData && Array.isArray(hotelsData)) {
        // For now, add all hotels from separate property to each event
        // Later you can add logic to match hotels to specific events
        hotelsToProcess = [...hotelsToProcess, ...hotelsData];
      }
      
      if (hotelsToProcess && Array.isArray(hotelsToProcess)) {
        hotelsToProcess.forEach(hotel => {
          // Try new dates_booked format first, then fallback to old check_in/check_out
          let hotelTimes = null;
          
          if (hotel.dates_booked) {
            hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
          } else if (hotel.check_in && hotel.check_out) {
            // Fallback to old format - keep local time
            try {
              hotelTimes = {
                start: hotel.check_in,
                end: hotel.check_out
              };
            } catch (e) {
              console.warn('Unable to parse hotel dates:', hotel.check_in, hotel.check_out);
              return;
            }
          }

          if (hotelTimes) {
            allCalendarEvents.push({
              type: 'hotel',
              title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
              start: hotelTimes.start,
              end: hotelTimes.end,
              description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation: ${hotel.names_on_reservation || 'N/A'}\nBooked Under: ${hotel.booked_under || 'N/A'}`,
              location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
              url: hotel.hotel_google_maps || hotel.hotel_apple_maps || '',
              confirmation: hotel.confirmation || '',
              hotelName: hotel.hotel_name || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add ground transport events
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

            // Format title to replace PICKUP/DROPOFF with proper capitalization
            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:');

            // Build description with formatted driver and passenger lists
            let description = '';
            
            // Add driver info
            if (transport.description) {
              // Extract driver and passenger info from description
              const driverMatch = transport.description.match(/Driver:\s*([^\n]+)/);
              const passengerMatch = transport.description.match(/Passenger:\s*([^\n]+)/);
              
              if (driverMatch) {
                description += `Driver: ${driverMatch[1]}\n\n`;
              }
              
              if (passengerMatch) {
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
              
              // Extract Driver Info section
              const driverInfoMatch = transport.description.match(/Driver Info:\s*([\s\S]*?)(?=\nPassenger Info:|Confirmation:|$)/);
              if (driverInfoMatch) {
                const driverInfo = driverInfoMatch[1].trim();
                if (driverInfo) {
                  description += 'Driver Info:\n';
                  // Format driver info with bullet points for each line
                  const driverInfoLines = driverInfo.split('\n').filter(line => line.trim());
                  driverInfoLines.forEach(line => {
                    const trimmedLine = line.trim();
                    if (trimmedLine) {
                      // Just display the original text as-is, no time parsing
                      description += `• ${trimmedLine}\n`;
                    }
                  });
                  description += '\n';
                }
              }
              
              // Extract Passenger Info section
              const passengerInfoMatch = transport.description.match(/Passenger Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
              if (passengerInfoMatch) {
                const passengerInfo = passengerInfoMatch[1].trim();
                if (passengerInfo) {
                  description += 'Passenger Info:\n';
                  // Format passenger info with bullet points for each line
                  const passengerInfoLines = passengerInfo.split('\n').filter(line => line.trim());
                  passengerInfoLines.forEach(line => {
                    const trimmedLine = line.trim();
                    if (trimmedLine) {
                      // Just display the original text as-is, no time parsing
                      description += `• ${trimmedLine}\n`;
                    }
                  });
                  description += '\n';
                }
              }
              
              // Add confirmation info if present
              const confirmationMatch = transport.description.match(/Confirmation:\s*([^\n]+)/);
              if (confirmationMatch) {
                description += `Confirmation: ${confirmationMatch[1]}\n`;
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

            // Check if there's a meetup location in Passenger Info and create separate meetup event
            if (transport.description) {
              const passengerInfoMatch = transport.description.match(/Passenger Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
              if (passengerInfoMatch) {
                const passengerInfo = passengerInfoMatch[1].trim();
                
                // Extract meetup location and time
                const meetupLocationMatch = passengerInfo.match(/Meetup Location:\s*([^\n]+)/);
                const meetupTimeMatch = passengerInfo.match(/Meetup Time:\s*([^\n]+)/);
                
                // Find address (usually the line after meetup location)
                const passengerInfoLines = passengerInfo.split('\n').filter(line => line.trim());
                let meetupAddress = '';
                if (meetupLocationMatch) {
                  const locationLineIndex = passengerInfoLines.findIndex(line => line.includes('Meetup Location:'));
                  if (locationLineIndex >= 0 && locationLineIndex + 1 < passengerInfoLines.length) {
                    const nextLine = passengerInfoLines[locationLineIndex + 1].trim();
                    // Check if next line looks like an address (contains numbers, street, city, state)
                    if (nextLine.match(/\d+.*(?:St|Ave|Blvd|Rd|Dr|Way).*(?:CA|NY|TX|FL)/)) {
                      meetupAddress = nextLine;
                    }
                  }
                }
                
                if (meetupLocationMatch && meetupTimeMatch) {
                  const meetupLocation = meetupLocationMatch[1].trim();
                  const meetupTime = meetupTimeMatch[1].trim();
                  
                  // Parse the meetup time using the same logic as ground transport
                  const parsedMeetupTime = parseUnifiedDateTime(`@${meetupTime}`);
                  if (parsedMeetupTime) {
                    const meetupStartTime = parsedMeetupTime.start;
                    const meetupEndTime = new Date(meetupStartTime.getTime() + 30 * 60 * 1000); // 30 minutes
                    
                    // Create meetup event description
                    let meetupDescription = `Meetup Location: ${meetupLocation}\n`;
                    if (meetupAddress) {
                      meetupDescription += `Address: ${meetupAddress}\n`;
                    }
                    meetupDescription += `\nBand members should meet here for pickup.`;
                    
                    // Add the meetup event
                    allCalendarEvents.push({
                      type: 'meetup',
                      title: `🚌 Meetup: Band (${event.event_name})`,
                      start: meetupStartTime.toISOString(),
                      end: meetupEndTime.toISOString(),
                      description: meetupDescription,
                      location: meetupAddress || meetupLocation,
                      mainEvent: event.event_name
                    });
                  }
                }
              }
            }
          }
        });
      }
    });
    
    if (shouldReturnICS) {
      // Generate ICS calendar with all events
      const personName = person.properties?.['Full Name']?.formula?.string || 'Unknown';
      const calendar = ical({ name: `${personName} - Downbeat Events` });

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
          url: event.url || ''
          // No timezone - let calendar apps interpret UTC times as local
        });
      });

      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="calendar.ics"');
      return res.send(calendar.toString());
    }

    // Return JSON format with expanded events
    res.json({
      personName: person.properties?.['Full Name']?.formula?.string || 'Unknown',
      totalMainEvents: events.length,
      totalCalendarEvents: allCalendarEvents.length,
      breakdown: {
        mainEvents: allCalendarEvents.filter(e => e.type === 'main_event').length,
        flights: allCalendarEvents.filter(e => e.type === 'flight_departure' || e.type === 'flight_return').length,
        rehearsals: allCalendarEvents.filter(e => e.type === 'rehearsal').length,
        hotels: allCalendarEvents.filter(e => e.type === 'hotel').length,
        groundTransport: allCalendarEvents.filter(e => e.type === 'ground_transport_pickup' || e.type === 'ground_transport_dropoff' || e.type === 'ground_transport').length
      },
      events: allCalendarEvents
    });
    
  } catch (error) {
    console.error('Calendar generation error:', error);
    res.status(500).json({ error: 'Error generating calendar' });
  }
});

app.listen(port, () => {
  console.log(`Calendar feed server running on port ${port}`);
});
