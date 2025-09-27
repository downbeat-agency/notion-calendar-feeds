import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';

const app = express();
const port = process.env.PORT || 3000; // Always-on Railway deployment
const notion = new Client({ auth: process.env.NOTION_API_KEY });

// --- DEBUG ROUTES ---
app.get('/debug/env', (_req, res) => {
  res.json({
    hasNotionKey: !!process.env.NOTION_API_KEY,
    keyPrefix: process.env.NOTION_API_KEY?.substring(0, 4) + '...',
    hasEventsDb: !!process.env.EVENTS_DATABASE_ID,
    hasPersonnelDb: !!process.env.PERSONNEL_DATABASE_ID,
    nodeEnv: process.env.NODE_ENV
  });
});

app.get('/debug/notion', async (_req, res) => {
  try {
    const who = await notion.users.me();
    res.json({ 
      ok: true, 
      user: who?.name || 'bot', 
      workspace: who?.bot?.owner?.workspace_name,
      clientMethods: {
        databases: Object.keys(notion.databases || {}),
        hasQuery: typeof notion.databases?.query === 'function'
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.body || e.message });
  }
});

async function dbInfo(id) {
  try {
    const meta = await notion.databases.retrieve({ database_id: id });
    const props = Object.entries(meta.properties || {}).map(([k,v]) => ({ name: k, type: v.type }));
    const sample = await notion.databases.query({ database_id: id, page_size: 1 });
    return { id, title: meta.title?.[0]?.plain_text, props, sampleCount: sample.results.length };
  } catch (error) {
    return { id, error: error.message, status: error.status };
  }
}

app.get('/debug/dbs', async (_req, res) => {
  try {
    const eventsId = process.env.EVENTS_DATABASE_ID;
    const peopleId = process.env.PERSONNEL_DATABASE_ID;
    
    if (!eventsId || !peopleId) {
      return res.status(400).json({ 
        ok: false, 
        error: 'Missing database IDs',
        eventsId: eventsId || 'MISSING',
        peopleId: peopleId || 'MISSING'
      });
    }
    
    const [events, personnel] = await Promise.all([dbInfo(eventsId), dbInfo(peopleId)]);
    res.json({ ok: true, events, personnel });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.body || e.message, stack: e.stack });
  }
});

const EVENTS_DB = '3dec3113-f747-49db-b666-8ba1f06c1d3e';
const PERSONNEL_DB = 'f8044a3d-6c88-4579-bbe0-2d15de3448be';

// Generate and update calendar URLs for all personnel
app.get('/update-calendar-urls', async (req, res) => {
  try {
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      sorts: [{ property: 'Full Name', direction: 'ascending' }]
    });

    let updated = 0;
    const baseUrl = `https://${req.get('host')}`;
    
    for (const person of response.results) {
      const personId = person.id;
      const calendarUrl = `${baseUrl}/calendar/${personId}`;
      
      // Update the person's record with their calendar URL
      await notion.pages.update({
        page_id: personId,
        properties: {
          'Calendar URL': {
            url: calendarUrl
          }
        }
      });
      updated++;
    }

    res.json({ 
      success: true, 
      message: `Updated ${updated} personnel records with calendar URLs`,
      baseUrl 
    });

  } catch (error) {
    console.error('Error updating calendar URLs:', error);
    res.status(500).json({ error: error.message });
  }
});

// Home page - List all personnel
app.get('/', async (req, res) => {
  try {
    // Get ALL personnel using pagination
    let allPersonnel = [];
    let hasMore = true;
    let startCursor = undefined;
    
    while (hasMore) {
      const queryParams = {
        database_id: PERSONNEL_DB,
        sorts: [{ property: 'Full Name', direction: 'ascending' }],
        page_size: 100
      };
      
      if (startCursor) {
        queryParams.start_cursor = startCursor;
      }
      
      const pageResponse = await notion.databases.query(queryParams);
      allPersonnel = allPersonnel.concat(pageResponse.results);
      
      hasMore = pageResponse.has_more;
      startCursor = pageResponse.next_cursor;
    }
    
    const response = { results: allPersonnel };
    
    const html = `
      <!DOCTYPE html>
      <html>
        <head>
          <title>Downbeat Calendar Feeds</title>
          <style>
            body {
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
              max-width: 800px;
              margin: 50px auto;
              padding: 20px;
              background: #f5f5f5;
            }
            h1 { color: #333; }
            .instructions {
              background: white;
              padding: 20px;
              border-radius: 8px;
              margin-bottom: 30px;
              box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .person {
              background: white;
              padding: 15px;
              margin: 10px 0;
              border-radius: 8px;
              display: flex;
              justify-content: space-between;
              align-items: center;
              box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .person:hover { background: #f8f9fa; }
            .name { font-weight: 600; font-size: 16px; }
            .button {
              background: #0066cc;
              color: white;
              padding: 10px 20px;
              border-radius: 6px;
              text-decoration: none;
              font-size: 14px;
            }
            .button:hover { background: #0052a3; }
          </style>
        </head>
        <body>
          <h1>🗓️ Downbeat Calendar Feeds</h1>
          
          <div class="instructions">
            <h3>How to subscribe:</h3>
            <ol>
              <li>Find your name below and <strong>right-click "Get Calendar"</strong></li>
              <li>Select <strong>"Copy Link Address"</strong></li>
              <li>In your calendar app:
                <ul>
                  <li><strong>Apple Calendar:</strong> File → New Calendar Subscription → Paste URL</li>
                  <li><strong>Google Calendar:</strong> Settings → Add calendar → From URL → Paste URL</li>
                  <li><strong>Outlook:</strong> Add calendar → Subscribe from web → Paste URL</li>
                </ul>
              </li>
              <li>Set refresh interval to 15 minutes for fastest updates</li>
            </ol>
          </div>
          
           <div>
             ${response.results.map(person => {
               const name = person.properties['Full Name']?.formula?.string || 'Unknown';
               const personId = person.id;
               const calendarUrl = person.properties['Calendar URL']?.url;
               
               return `
                 <div class="person">
                   <span class="name">${name}</span>
                   ${calendarUrl ? 
                     `<a href="${calendarUrl}" class="button">Get Calendar</a>` :
                     `<a href="/calendar/${personId}" class="button">Get Calendar</a>`
                   }
                 </div>
               `;
             }).join('')}
           </div>
           
           <div style="margin-top: 30px; padding: 20px; background: #fff3cd; border-radius: 8px;">
             <h3>📝 Admin</h3>
             <p>To populate calendar URLs in your Notion database:</p>
             <a href="/update-calendar-urls" style="background: #28a745; color: white; padding: 10px 20px; border-radius: 6px; text-decoration: none;">Update All Calendar URLs</a>
           </div>
        </body>
      </html>
    `;
    
    res.send(html);
  } catch (error) {
    console.error('Error:', error);
    res.status(500).send('Error loading personnel');
  }
});

// Debug all events in database (first 10 to see structure)
app.get('/debug/events', async (req, res) => {
  try {
    // Get first page to start
    let allResults = [];
    let hasMore = true;
    let startCursor = undefined;
    let pageCount = 0;
    const maxPages = 10; // Safety limit to avoid infinite loops

    while (hasMore && pageCount < maxPages) {
      const response = await notion.databases.query({
        database_id: EVENTS_DB,
        page_size: 100,
        sorts: [{ property: 'Event Date', direction: 'descending' }],
        start_cursor: startCursor
      });
      
      allResults = allResults.concat(response.results);
      hasMore = response.has_more;
      startCursor = response.next_cursor;
      pageCount++;
    }
    
    const eventDebug = allResults.slice(0, 50).map(event => { // Still limit display to 50 for readability
      const props = event.properties;
      return {
        id: event.id,
        title: props.Event?.title?.[0]?.plain_text,
        eventDate: props['Event Date']?.date,
        location: props['Location (Print)']?.rich_text?.[0]?.plain_text,
        eventType: props['Event Type']?.select?.name,
        payrollPersonnelIds: props['Payroll Personnel']?.relation?.map(rel => rel.id) || [],
        payrollPersonnelCount: props['Payroll Personnel']?.relation?.length || 0
      };
    });

    res.json({
      totalEventsInDB: allResults.length,
      totalPagesQueried: pageCount,
      events: eventDebug,
      eventsDisplayed: eventDebug.length,
      hasMore: allResults.length > 50
    });
    
  } catch (error) {
    console.error('Error debugging events:', error);
    res.status(500).json({ error: error.message });
  }
});

// Debug calendar data for specific person  
app.get('/debug/calendar/:personId', async (req, res) => {
  let { personId } = req.params;
  
  // Convert personId to proper UUID format if it doesn't have hyphens
  if (personId.length === 32 && !personId.includes('-')) {
    personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }
  
  try {
    // Get the person's record and their Calendar Feed JSON
    const person = await notion.pages.retrieve({ page_id: personId });
    const calendarFeedJson = person.properties?.['Calendar Feed JSON']?.formula?.string || 
                             person.properties?.['Calendar Feed JSON']?.rich_text?.[0]?.plain_text;
    
    if (!calendarFeedJson) {
      return res.json({
        searchingForPersonId: personId,
        personPageId: person.id,
        personName: person.properties?.['Full Name']?.formula?.string || person.properties?.['Nickname']?.title?.[0]?.plain_text,
        totalEvents: 0,
        events: [],
        filterApproach: 'calendar-feed-json',
        error: 'No Calendar Feed JSON property found or formula returned empty',
        propertyType: person.properties?.['Calendar Feed JSON']?.type,
        propertyValue: person.properties?.['Calendar Feed JSON'],
        availableProperties: Object.keys(person.properties)
      });
    }
    
    // Parse the calendar feed JSON
    let calendarData;
    try {
      // Clean up common JSON issues like unquoted dollar amounts
      const cleanedJson = calendarFeedJson.replace(/:\$(\d+)/g, ':"$$$1"');
      calendarData = JSON.parse(cleanedJson);
    } catch (parseError) {
      return res.json({
        searchingForPersonId: personId,
        personPageId: person.id,
        personName: person.properties?.['Full Name']?.formula?.string || person.properties?.['Nickname']?.title?.[0]?.plain_text,
        totalEvents: 0,
        events: [],
        filterApproach: 'calendar-feed-json',
        error: 'Invalid JSON in Calendar Feed JSON property',
        parseError: parseError.message,
        rawJson: calendarFeedJson?.substring(0, 200) + '...'
      });
    }
    
    // Handle different JSON structures
    let events = [];
    if (Array.isArray(calendarData)) {
      events = calendarData; // JSON is directly an array
    } else if (calendarData.events) {
      events = calendarData.events; // JSON has events property
    } else {
      events = []; // No recognizable structure
    }
    
    const response = { results: events };
    const filterApproach = 'calendar-feed-json';
    
    // Debug the events from JSON data
    const eventDebug = response.results.map((event, index) => {
      return {
        index,
        event_name: event.event_name,
        event_date: event.event_date,
        event_start: event.event_start,
        event_end: event.event_end,
        band: event.band,
        venue: event.venue,
        venue_address: event.venue_address,
        general_info: event.general_info || 'Not available',
        general_info_length: event.general_info ? event.general_info.length : 0,
        has_general_info: !!event.general_info,
        rehearsals: event.rehearsals || [],
        rehearsalCount: event.rehearsals ? event.rehearsals.length : 0,
        cleanedStartDate: event.event_start?.replace(/[']/g, ''),
        cleanedEndDate: event.event_end?.replace(/[']/g, '')
      };
    });
    
    res.json({
      searchingForPersonId: personId,
      personPageId: person.id,
      personName: person.properties?.['Full Name']?.formula?.string || person.properties?.['Nickname']?.title?.[0]?.plain_text,
      totalEvents: response.results.length,
      events: eventDebug,
      filterApproach,
      debugInfo: {
        hasCalendarFeedJson: !!calendarFeedJson,
        jsonLength: calendarFeedJson?.length || 0,
        parsedSuccessfully: true,
        rawJsonFromNotion: calendarFeedJson, // Show the exact raw JSON from Notion API
        jsonStructure: {
          isArray: Array.isArray(calendarData),
          hasEventsProperty: !!calendarData.events,
          topLevelKeys: Object.keys(calendarData).slice(0, 10),
          firstEventSample: events[0] || null
        },
        propertyDebug: {
          propertyExists: !!person.properties?.['Calendar Feed JSON'],
          propertyType: person.properties?.['Calendar Feed JSON']?.type,
          formulaExists: !!person.properties?.['Calendar Feed JSON']?.formula,
          formulaString: person.properties?.['Calendar Feed JSON']?.formula?.string?.substring(0, 200) + '...'
        }
      },
      idComparison: {
        searchId: personId,
        actualPersonPageId: person.id,
        idsMatch: personId === person.id
      }
    });
    
  } catch (error) {
    console.error('Error debugging calendar:', error);
    res.status(500).json({ error: error.message, personId });
  }
});

// Debug personnel database to see actual IDs
app.get('/debug/personnel', async (req, res) => {
  try {
    let allPersonnel = [];
    let hasMore = true;
    let startCursor = undefined;
    
    while (hasMore) {
      const queryParams = {
        database_id: PERSONNEL_DB,
        sorts: [{ property: 'Full Name', direction: 'ascending' }],
        page_size: 100
      };
      
      if (startCursor) {
        queryParams.start_cursor = startCursor;
      }
      
      const response = await notion.databases.query(queryParams);
      allPersonnel = allPersonnel.concat(response.results);
      
      hasMore = response.has_more;
      startCursor = response.next_cursor;
      
      if (allPersonnel.length > 200) break; // Safety limit
    }
    
    const personnelDebug = allPersonnel.map(person => ({
      id: person.id,
      fullName: person.properties?.['Full Name']?.formula?.string,
      nickname: person.properties?.['Nickname']?.title?.[0]?.plain_text,
      email: person.properties?.['Email']?.email
    }));
    
    res.json({
      totalPersonnel: allPersonnel.length,
      personnel: personnelDebug,
      andrewSearch: personnelDebug.find(p => 
        p.fullName?.toLowerCase().includes('andrew') || 
        p.nickname?.toLowerCase().includes('andrew')
      )
    });
    
  } catch (error) {
    console.error('Error debugging personnel:', error);
    res.status(500).json({ error: error.message });
  }
});

// Debug specific personnel IDs
app.get('/debug/lookup-personnel', async (req, res) => {
  try {
    // IDs from the AMFM Orange County Wedding event
    const eventPersonnelIds = [
      "26d39e4a-65a9-8113-88e5-d3c11a5c7810",
      "26d39e4a-65a9-8124-b819-ee55fb435216", 
      "26d39e4a-65a9-81d0-828d-eb0538122e3f",
      "26d39e4a-65a9-818e-9d19-ddfbb3cc1aa7",
      "26d39e4a-65a9-81c9-b3f3-dbc146661a8b",
      "26d39e4a-65a9-8185-9488-c572478b9137",
      "26d39e4a-65a9-81ed-b492-dfbe4dbb48b0",
      "26d39e4a-65a9-817a-b3fc-c093ad56fb67",
      "26d39e4a-65a9-8199-aba1-fdfdb33ce324",
      "26d39e4a-65a9-81f9-8faa-df7f9b9caa29",
      "26d39e4a-65a9-81c9-803d-cf8fbc01aa65",
      "26d39e4a-65a9-8192-a3cb-ce1013ab352c"
    ];
    
    const andrewSearchId = "345984c3-1f94-4476-a27c-1b98f51c56d8";
    
    // Look up each personnel ID
    const personnelLookups = [];
    
    for (const personId of eventPersonnelIds) {
      try {
        const person = await notion.pages.retrieve({ page_id: personId });
        personnelLookups.push({
          id: personId,
          fullName: person.properties?.['Full Name']?.formula?.string,
          nickname: person.properties?.['Nickname']?.title?.[0]?.plain_text,
          email: person.properties?.['Email']?.email,
          isAndrew: personId === andrewSearchId,
          matchesAndrewSearch: personId === andrewSearchId
        });
      } catch (error) {
        personnelLookups.push({
          id: personId,
          error: error.message,
          isAndrew: personId === andrewSearchId
        });
      }
    }
    
    // Also try to look up Andrew's ID
    let andrewLookup = null;
    try {
      const andrew = await notion.pages.retrieve({ page_id: andrewSearchId });
      andrewLookup = {
        id: andrewSearchId,
        fullName: andrew.properties?.['Full Name']?.formula?.string,
        nickname: andrew.properties?.['Nickname']?.title?.[0]?.plain_text,
        email: andrew.properties?.['Email']?.email,
        inEventList: eventPersonnelIds.includes(andrewSearchId)
      };
    } catch (error) {
      andrewLookup = {
        id: andrewSearchId,
        error: error.message,
        inEventList: false
      };
    }
    
    res.json({
      eventTitle: "11/21/26 AMFM - Orange County Wedding",
      eventPersonnelCount: eventPersonnelIds.length,
      andrewSearchId,
      andrewInEventList: eventPersonnelIds.includes(andrewSearchId),
      andrewLookup,
      eventPersonnel: personnelLookups,
      andrewMatches: personnelLookups.filter(p => p.matchesAndrewSearch),
      andrewNameMatches: personnelLookups.filter(p => 
        p.fullName?.toLowerCase().includes('andrew') || 
        p.nickname?.toLowerCase().includes('andrew')
      )
    });
    
  } catch (error) {
    console.error('Error looking up personnel:', error);
    res.status(500).json({ error: error.message });
  }
});

// Simple health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    message: 'Server is running' 
  });
});

// Test flight implementation with sample data
app.get('/test/flights', async (req, res) => {
  try {
    // Use your provided sample data with flight information
    const sampleEvents = [
      {
        "event_name": "Kapolei Wedding",
        "notion_url": "https://www.notion.so/1a639e4a65a980889e0af5ee5b096ce8",
        "event_date": "2025-09-06T15:30:00-07:00",
        "event_start": "2025-09-06T15:30:00-07:00",
        "event_end": "2025-09-06T22:00:00-07:00",
        "band": "Project 21",
        "general_info": "Parking and Load In:\nLoad in directly to the venue from the parking lot. All one level. No elevators. Dollys and handtrucks are recommended.\n\nLarge free parking area. Please note dinner and dancing will be under a sail cloth tent.\nADDITIONAL POC/On-Site Manager: Maile Hatfield 415-302-1391\n\nDress Code:\nGentleman - black suit, white shirt, black bow tie, black dress shoes\nLadies - black formal dress, black dress shoes\n\nGreen Room:\nA Green room tent will be provided at Phase 2 for the band\n\nDay of Contact for Band: Hubie Wang - +14153052104\n\nContracted:\nCeremony: Guitar + Ceremony Sound 330pm-430pm (1hrs)\nCocktail: Bass + Keys + Cocktail Sound + Sax 430pm-545pm (1.25hrs)\nReception: Reception Sound + Band + Horns 545pm-10pm (4.25hrs)\n\nLocations:\nCeremony Outdoor\nCocktail Outdoor\nDinner Outdoor\nReception Outdoor\n\nContract Updated: February 25, 2025 8:19 PM\n\nNotes Updated: September 5, 2025 10:32 AM\n\nTimeline Updated: ",
        "venue": "Lanikūhonua Cultural Institute",
        "venue_address": "92-1101 Ali'inui Drive Kapolei, HI 96707",
        "payroll": [{"position": "Band - Vox 1", "assignment": "Base + Rehearsal + A3 + Cocktail + Per Diem + Additional", "pay_total": 2425}],
        "rehearsals": [{"rehearsal_time": "2025-09-03T14:00:00-07:00", "rehearsal_location": "Downbeat HQ", "rehearsal_address": "123 W Bellevue Dr Ste 4,\nPasadena, CA 91105⁠"}],
        "flights": [
          {
            "confirmation": "HEOOAO",
            "flight_status": "Booked",
            "flight_type": "Round Trip",
            "departure_name": "Flight to HNL (Band)",
            "departure_airline": "American Airlines",
            "departure_flightnumber": "AA 31",
            "departure_from": "LAX",
            "departure_from_city": "@Los Angeles",
            "departure_to": "HNL",
            "departure_to_city": "@Honolulu",
            "departure_time": "2025-09-05T08:48:00-07:00",
            "departure_arrival_time": "2025-09-05T11:45:00-07:00",
            "departure_duration": "10620",
            "return_confirmation": null,
            "return_name": "Flight Return to LAX (Band)",
            "return_airline": null,
            "return_flightnumber": "AA 164",
            "return_from": "HNL",
            "return_from_city": "",
            "return_to": "LAX",
            "return_to_city": "",
            "return_time": "2025-09-07T15:26:00-07:00",
            "return_arrival_time": "2025-09-07T23:58:00-07:00",
            "return_duration": "30720"
          }
        ],
        "pay_total": 2425
      }
    ];

    const calendar = ical({ 
      name: 'Flight Test Calendar',
      timezone: 'America/Los_Angeles'
    });

    const eventSummary = [];

    sampleEvents.forEach((event, index) => {
      const title = event.event_name;
      const startDate = event.event_start;
      const endDate = event.event_end;
      const venue = event.venue;
      const venueAddress = event.venue_address;
      const band = event.band;
      const eventId = `event-${index}-${Date.now()}`;

      // Create location string from venue and address
      const location = venueAddress ? `${venue}, ${venueAddress}` : venue;

      if (startDate && title) {
        // Fix date format by replacing 'T' quotes with actual T
        const cleanStartDate = startDate.replace(/[']/g, '');
        const cleanEndDate = endDate ? endDate.replace(/[']/g, '') : cleanStartDate;

        // Build position assignments and pay information for test
        let testPositionInfo = '';
        if (event.payroll && event.payroll.length > 0) {
          testPositionInfo = event.payroll.map(payrollItem => {
            // Handle pay that might already have $ symbol or be just a number
            const payAmount = typeof payrollItem.pay_total === 'string' ? 
              (payrollItem.pay_total.startsWith('$') ? payrollItem.pay_total : `$${payrollItem.pay_total}`) : 
              `$${payrollItem.pay_total}`;
            return `Position: ${payrollItem.position}\nPosition Assignments:\n${payrollItem.assignment} - ${payAmount}`;
          }).join('\n\n');
        }

        // Build description starting with position info, then general_info
        let testEventDescription = '';
        
        // Add position info first if available
        if (testPositionInfo) {
          testEventDescription = testPositionInfo;
        }
        
        // Add general_info after position info, or fall back to band/venue format
        if (event.general_info) {
          const generalInfo = event.general_info
            .replace(/\r\n/g, '\n')  // Normalize line endings
            .replace(/\r/g, '\n')    // Normalize line endings
            .trim(); // Remove leading/trailing whitespace
          
          if (testEventDescription) {
            testEventDescription += '\n\n' + generalInfo;
          } else {
            testEventDescription = generalInfo;
          }
        } else {
          const fallbackInfo = `Band: ${band}\nVenue: ${venue}`;
          if (testEventDescription) {
            testEventDescription += '\n\n' + fallbackInfo;
          } else {
            testEventDescription = fallbackInfo;
          }
        }

        // Create main event
        calendar.createEvent({
          start: new Date(cleanStartDate),
          end: new Date(cleanEndDate),
          summary: `${title} (${band})`,
          location: location || '',
          description: testEventDescription,
          uid: `${eventId}@downbeat.agency`,
          url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
        });

        const mainEventInfo = {
          type: 'main_event',
          title: `${title} (${band})`,
          start: cleanStartDate,
          end: cleanEndDate,
          location: location
        };

        eventSummary.push(mainEventInfo);

        // Add flight events if they exist (using same logic as main calendar)
        if (event.flights && Array.isArray(event.flights)) {
          event.flights.forEach((flight, flightIndex) => {
            // Process departure flight
            if (flight.departure_name || flight.departure_airline) {
              // Use actual flight times if available, otherwise use placeholder times
              let departureStart, departureEnd;
              
              if (flight.departure_time) {
                // Use real departure time from Notion
                departureStart = new Date(flight.departure_time);
                departureEnd = flight.departure_arrival_time ? 
                  new Date(flight.departure_arrival_time) : 
                  new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                departureStart = new Date(new Date(event.event_date).getTime() - 24 * 60 * 60 * 1000); // 1 day before event
                departureStart.setHours(10, 0, 0, 0); // Set to 10:00 AM
                departureEnd = new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const departureEventId = `flight-departure-${index}-${flightIndex}-${Date.now()}`;
              
              // Create departure airport location
              const departureLocation = flight.departure_from_city ? 
                `${flight.departure_from} (${flight.departure_from_city.replace('@', '')})` : 
                flight.departure_from;
              
              const arrivalLocation = flight.departure_to_city ? 
                `${flight.departure_to} (${flight.departure_to_city.replace('@', '')})` : 
                flight.departure_to;
              
              // Build flight description
              let flightDescription = `Flight for: ${title}\nBand: ${band}\n`;
              flightDescription += `Airline: ${flight.departure_airline || 'TBD'}\n`;
              flightDescription += `Flight: ${flight.departure_flightnumber || 'TBD'}\n`;
              flightDescription += `From: ${departureLocation}\n`;
              flightDescription += `To: ${arrivalLocation}\n`;
              if (flight.confirmation) {
                flightDescription += `Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                flightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: departureStart,
                end: departureEnd,
                summary: `✈️ FLIGHT: ${flight.departure_name || `${departureLocation} → ${arrivalLocation}`}`,
                location: departureLocation,
                description: flightDescription,
                uid: `${departureEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });

              const departureInfo = {
                type: 'flight_departure',
                title: `✈️ FLIGHT: ${flight.departure_name || `${departureLocation} → ${arrivalLocation}`}`,
                start: departureStart.toISOString(),
                end: departureEnd.toISOString(),
                location: departureLocation,
                airline: flight.departure_airline,
                flightNumber: flight.departure_flightnumber,
                confirmation: flight.confirmation,
                mainEvent: title,
                duration: `${Math.round((departureEnd - departureStart) / (1000 * 60))} minutes`
              };

              eventSummary.push(departureInfo);
            }

            // Process return flight
            if (flight.return_name || flight.return_airline) {
              // Use actual return flight times if available, otherwise use placeholder times
              let returnStart, returnEnd;
              
              if (flight.return_time) {
                // Use real return time from Notion
                returnStart = new Date(flight.return_time);
                returnEnd = flight.return_arrival_time ? 
                  new Date(flight.return_arrival_time) : 
                  new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                returnStart = new Date(new Date(event.event_date).getTime() + 24 * 60 * 60 * 1000); // 1 day after event
                returnStart.setHours(14, 0, 0, 0); // Set to 2:00 PM
                returnEnd = new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const returnEventId = `flight-return-${index}-${flightIndex}-${Date.now()}`;
              
              // Create return airport location
              const returnDepartureLocation = flight.return_from_city ? 
                `${flight.return_from} (${flight.return_from_city.replace('@', '')})` : 
                flight.return_from;
              
              const returnArrivalLocation = flight.return_to_city ? 
                `${flight.return_to} (${flight.return_to_city.replace('@', '')})` : 
                flight.return_to;
              
              // Build return flight description
              let returnFlightDescription = `Return flight for: ${title}\nBand: ${band}\n`;
              returnFlightDescription += `Airline: ${flight.return_airline || flight.departure_airline || 'TBD'}\n`;
              returnFlightDescription += `Flight: ${flight.return_flightnumber || 'TBD'}\n`;
              returnFlightDescription += `From: ${returnDepartureLocation}\n`;
              returnFlightDescription += `To: ${returnArrivalLocation}\n`;
              if (flight.return_confirmation || flight.confirmation) {
                returnFlightDescription += `Confirmation: ${flight.return_confirmation || flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                returnFlightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: returnStart,
                end: returnEnd,
                summary: `✈️ RETURN: ${flight.return_name || `${returnDepartureLocation} → ${returnArrivalLocation}`}`,
                location: returnDepartureLocation,
                description: returnFlightDescription,
                uid: `${returnEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });

              const returnInfo = {
                type: 'flight_return',
                title: `✈️ RETURN: ${flight.return_name || `${returnDepartureLocation} → ${returnArrivalLocation}`}`,
                start: returnStart.toISOString(),
                end: returnEnd.toISOString(),
                location: returnDepartureLocation,
                airline: flight.return_airline || flight.departure_airline,
                flightNumber: flight.return_flightnumber,
                confirmation: flight.return_confirmation || flight.confirmation,
                mainEvent: title,
                duration: `${Math.round((returnEnd - returnStart) / (1000 * 60))} minutes`
              };

              eventSummary.push(returnInfo);
            }
          });
        }
      }
    });

    // Return both JSON summary and option to download .ics
    if (req.query.format === 'ics') {
      res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="flight-test.ics"');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.send(calendar.toString());
    } else {
      res.json({
        message: 'Flight test completed successfully',
        totalEvents: eventSummary.filter(e => e.type === 'main_event').length,
        totalFlights: eventSummary.filter(e => e.type.startsWith('flight_')).length,
        totalDepartureFlights: eventSummary.filter(e => e.type === 'flight_departure').length,
        totalReturnFlights: eventSummary.filter(e => e.type === 'flight_return').length,
        events: eventSummary,
        downloadIcs: `${req.protocol}://${req.get('host')}/test/flights?format=ics`
      });
    }

  } catch (error) {
    console.error('Error testing flights:', error);
    res.status(500).json({ error: error.message });
  }
});

// Test rehearsal implementation with sample data
app.get('/test/rehearsals', async (req, res) => {
  try {
    // Sample data with general_info field
    const sampleEvents = [{"event_name":"San Diego Wedding","notion_url":"https://www.notion.so/c5891334e26a4321ba1f708433fe2cd0","event_date":"2025-08-30T15:30:00-07:00","event_start":"2025-08-30T15:30:00-07:00","event_end":"2025-08-31T00:00:00-07:00","band":"AMFM","general_info":"Parking and Load In:\nLoad In: Please do not park in the red denoted areas (attached) on North Torrey Pines Road.• Please use the loading dock areas denoted below in green, do not leave vehicles here for extended periods of time.• If the area in green is full, vendors MUST remain on Callan Rd or N. Torrey Pines Place until the area is free to unload. After vendors are finished loading in, they may self-park complimentary on level 4 or 5 in the hotel parking garage if the vehicle fits in the garage (clearance is 7ft)\n\nSmall Cars: Please use the self-parking garage, self-parking complimentary based upon availability.\n\nDress Code:\nGentleman - black suit, white shirt, black bow tie, black dress shoes\nLadies - black formal dress, black dress shoes\n\nGreen Room:\nYes. Room TBD\n\nDay of Contact for Band: Andrew Reyes - (626) 482-8315,(203) 300-4278\n\nContracted:\nCeremony: Ceremony Sound + String Trio (Dolce) + Tech 330pm-430pm (1hrs)\nCocktail: Cocktail Sound + Guitar + Keys 430pm-6pm (1.5hrs)\nDinner: Ceremony Sound 6pm-8pm (2hrs)\nReception: Reception Sound + Band + Horns 6pm-10pm (4hrs)\nAfter Party: Reception Sound + DJ 10pm-12am (2hrs)\n\nLocations:\nCeremony Outdoor\nCocktail Outdoor\nReception Indoor\nAfter Party Indoor\nDinner Outdoor\n\nContract Updated: May 13, 2025 2:41 PM\n\nNotes Updated: August 27, 2025 8:12 PM\n\nTimeline Updated: ","venue":"The Lodge at Torrey Pines","venue_address":"11480 N Torrey Pines Rd, La Jolla, CA 92037","payroll":[{"position":"Band: Keys","assignment":"Base + Rehearsal + MD + DJ + A4 + Cocktail + After Party","pay_total":2550}],"rehearsals":[{"rehearsal_time":"@August 28, 2025 12:00 PM → 2:00 PM","rehearsal_location":"Classic Room","rehearsal_address":""}]}];

    const calendar = ical({ 
      name: 'Rehearsal Test Calendar',
      timezone: 'America/Los_Angeles'
    });

    const eventSummary = [];

    sampleEvents.forEach((event, index) => {
      const title = event.event_name;
      const startDate = event.event_start;
      const endDate = event.event_end;
      const venue = event.venue;
      const venueAddress = event.venue_address;
      const band = event.band;
      const eventId = `event-${index}-${Date.now()}`;

      // Create location string from venue and address
      const location = venueAddress ? `${venue}, ${venueAddress}` : venue;

      if (startDate && title) {
        // Fix date format by replacing 'T' quotes with actual T
        const cleanStartDate = startDate.replace(/[']/g, '');
        const cleanEndDate = endDate ? endDate.replace(/[']/g, '') : cleanStartDate;

        // Build position assignments and pay information for test
        let testPositionInfo = '';
        if (event.payroll && event.payroll.length > 0) {
          testPositionInfo = event.payroll.map(payrollItem => {
            // Handle pay that might already have $ symbol or be just a number
            const payAmount = typeof payrollItem.pay_total === 'string' ? 
              (payrollItem.pay_total.startsWith('$') ? payrollItem.pay_total : `$${payrollItem.pay_total}`) : 
              `$${payrollItem.pay_total}`;
            return `Position: ${payrollItem.position}\nPosition Assignments:\n${payrollItem.assignment} - ${payAmount}`;
          }).join('\n\n');
        }

        // Build description starting with position info, then general_info
        let testEventDescription = '';
        
        // Add position info first if available
        if (testPositionInfo) {
          testEventDescription = testPositionInfo;
        }
        
        // Add general_info after position info, or fall back to band/venue format
        if (event.general_info) {
          const generalInfo = event.general_info
            .replace(/\r\n/g, '\n')  // Normalize line endings
            .replace(/\r/g, '\n')    // Normalize line endings
            .trim(); // Remove leading/trailing whitespace
          
          if (testEventDescription) {
            testEventDescription += '\n\n' + generalInfo;
          } else {
            testEventDescription = generalInfo;
          }
        } else {
          const fallbackInfo = `Band: ${band}\nVenue: ${venue}`;
          if (testEventDescription) {
            testEventDescription += '\n\n' + fallbackInfo;
          } else {
            testEventDescription = fallbackInfo;
          }
        }

        // Create main event
        calendar.createEvent({
          start: new Date(cleanStartDate),
          end: new Date(cleanEndDate),
          summary: `${title} (${band})`,
          location: location || '',
          description: testEventDescription,
          uid: `${eventId}@downbeat.agency`,
          url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
        });

        const mainEventInfo = {
          type: 'main_event',
          title: `${title} (${band})`,
          start: cleanStartDate,
          end: cleanEndDate,
          location: location
        };

        eventSummary.push(mainEventInfo);

        // Add rehearsal events if they exist
        if (event.rehearsals && Array.isArray(event.rehearsals)) {
          event.rehearsals.forEach((rehearsal, rehearsalIndex) => {
            if (rehearsal.rehearsal_time) {
              let rehearsalStart, rehearsalEnd;
              
              // Handle different rehearsal_time formats
              const rehearsalTimeStr = rehearsal.rehearsal_time.replace(/[']/g, '');
              
              if (rehearsalTimeStr.includes('→')) {
                // Format: "@Month DD, YYYY H:MM AM/PM (TZ) → H:MM AM/PM"
                const match = rehearsalTimeStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s+→\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
                if (match) {
                  const dateStr = match[1].trim(); // "September 11, 2025"
                  const startTimeStr = match[2].trim(); // "10:00 AM"
                  const endTimeStr = match[3].trim(); // "12:00 PM"
                  
                  // Parse the date and times
                  rehearsalStart = new Date(`${dateStr} ${startTimeStr}`);
                  rehearsalEnd = new Date(`${dateStr} ${endTimeStr}`);
                  
                  // Ensure valid dates
                  if (isNaN(rehearsalStart.getTime()) || isNaN(rehearsalEnd.getTime())) {
                    // Fallback to 3 hours if parsing fails
                    rehearsalStart = new Date(rehearsalTimeStr);
                    rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000);
                  }
                } else {
                  // Fallback if regex doesn't match
                  rehearsalStart = new Date(rehearsalTimeStr);
                  rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000);
                }
              } else if (rehearsalTimeStr.includes('/')) {
                // Format: "start/end" (e.g., "2025-09-11T10:00:00-07:00/2025-09-11T13:00:00-07:00")
                const [startStr, endStr] = rehearsalTimeStr.split('/');
                rehearsalStart = new Date(startStr.trim());
                rehearsalEnd = new Date(endStr.trim());
              } else if (rehearsal.rehearsal_start && rehearsal.rehearsal_end) {
                // Separate start/end fields
                rehearsalStart = new Date(rehearsal.rehearsal_start.replace(/[']/g, ''));
                rehearsalEnd = new Date(rehearsal.rehearsal_end.replace(/[']/g, ''));
              } else if (rehearsal.rehearsal_time && rehearsal.rehearsal_end) {
                // Start time in rehearsal_time, end time separate
                rehearsalStart = new Date(rehearsalTimeStr);
                rehearsalEnd = new Date(rehearsal.rehearsal_end.replace(/[']/g, ''));
              } else {
                // Only start time provided, default to 3 hours
                rehearsalStart = new Date(rehearsalTimeStr);
                rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000); // Add 3 hours
              }
              
              const rehearsalEventId = `rehearsal-${index}-${rehearsalIndex}-${Date.now()}`;
              const rehearsalLocationName = rehearsal.rehearsal_location || 'TBD';
              const rehearsalAddress = rehearsal.rehearsal_address || '';
              
              // Create full location string (name + address)
              let fullRehearsalLocation = rehearsalLocationName;
              if (rehearsalAddress) {
                fullRehearsalLocation = `${rehearsalLocationName}, ${rehearsalAddress}`;
              }
              
              calendar.createEvent({
                start: rehearsalStart,
                end: rehearsalEnd,
                summary: `REHEARSAL: ${title} (${band})`,
                location: fullRehearsalLocation,
                description: `Rehearsal for: ${title}\nBand: ${band}\nMain Event Venue: ${venue}`,
                uid: `${rehearsalEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });

              const rehearsalInfo = {
                type: 'rehearsal',
                title: `REHEARSAL: ${title} (${band})`,
                start: rehearsalStart.toISOString(),
                end: rehearsalEnd.toISOString(),
                location: fullRehearsalLocation,
                locationName: rehearsalLocationName,
                address: rehearsalAddress,
                mainEvent: title,
                duration: `${Math.round((rehearsalEnd - rehearsalStart) / (1000 * 60))} minutes`
              };

              eventSummary.push(rehearsalInfo);
            }
          });
        }

        // Add flight events if they exist
        if (event.flights && Array.isArray(event.flights)) {
          event.flights.forEach((flight, flightIndex) => {
            // Process departure flight
            if (flight.departure_name || flight.departure_airline) {
              // Use actual flight times if available, otherwise use placeholder times
              let departureStart, departureEnd;
              
              if (flight.departure_time) {
                // Use real departure time from Notion
                departureStart = new Date(flight.departure_time);
                departureEnd = flight.departure_arrival_time ? 
                  new Date(flight.departure_arrival_time) : 
                  new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                departureStart = new Date(new Date(event.event_date).getTime() - 24 * 60 * 60 * 1000); // 1 day before event
                departureStart.setHours(10, 0, 0, 0); // Set to 10:00 AM
                departureEnd = new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const departureEventId = `flight-departure-${index}-${flightIndex}-${Date.now()}`;
              
              // Create departure airport location
              const departureLocation = flight.departure_from_city ? 
                `${flight.departure_from} (${flight.departure_from_city.replace('@', '')})` : 
                flight.departure_from;
              
              const arrivalLocation = flight.departure_to_city ? 
                `${flight.departure_to} (${flight.departure_to_city.replace('@', '')})` : 
                flight.departure_to;
              
              // Build flight description
              let flightDescription = `Flight for: ${title}\nBand: ${band}\n`;
              flightDescription += `Airline: ${flight.departure_airline || 'TBD'}\n`;
              flightDescription += `Flight: ${flight.departure_flightnumber || 'TBD'}\n`;
              flightDescription += `From: ${departureLocation}\n`;
              flightDescription += `To: ${arrivalLocation}\n`;
              if (flight.confirmation) {
                flightDescription += `Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                flightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: departureStart,
                end: departureEnd,
                summary: `✈️ FLIGHT: ${flight.departure_name || `${departureLocation} → ${arrivalLocation}`}`,
                location: departureLocation,
                description: flightDescription,
                uid: `${departureEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });

              const departureInfo = {
                type: 'flight_departure',
                title: `✈️ FLIGHT: ${flight.departure_name || `${departureLocation} → ${arrivalLocation}`}`,
                start: departureStart.toISOString(),
                end: departureEnd.toISOString(),
                location: departureLocation,
                airline: flight.departure_airline,
                flightNumber: flight.departure_flightnumber,
                confirmation: flight.confirmation,
                mainEvent: title,
                duration: `${Math.round((departureEnd - departureStart) / (1000 * 60))} minutes`
              };

              eventSummary.push(departureInfo);
            }

            // Process return flight
            if (flight.return_name || flight.return_airline) {
              // Use actual return flight times if available, otherwise use placeholder times
              let returnStart, returnEnd;
              
              if (flight.return_time) {
                // Use real return time from Notion
                returnStart = new Date(flight.return_time);
                returnEnd = flight.return_arrival_time ? 
                  new Date(flight.return_arrival_time) : 
                  new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                returnStart = new Date(new Date(event.event_date).getTime() + 24 * 60 * 60 * 1000); // 1 day after event
                returnStart.setHours(14, 0, 0, 0); // Set to 2:00 PM
                returnEnd = new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const returnEventId = `flight-return-${index}-${flightIndex}-${Date.now()}`;
              
              // Create return airport location
              const returnDepartureLocation = flight.return_from_city ? 
                `${flight.return_from} (${flight.return_from_city.replace('@', '')})` : 
                flight.return_from;
              
              const returnArrivalLocation = flight.return_to_city ? 
                `${flight.return_to} (${flight.return_to_city.replace('@', '')})` : 
                flight.return_to;
              
              // Build return flight description
              let returnFlightDescription = `Return flight for: ${title}\nBand: ${band}\n`;
              returnFlightDescription += `Airline: ${flight.return_airline || flight.departure_airline || 'TBD'}\n`;
              returnFlightDescription += `Flight: ${flight.return_flightnumber || 'TBD'}\n`;
              returnFlightDescription += `From: ${returnDepartureLocation}\n`;
              returnFlightDescription += `To: ${returnArrivalLocation}\n`;
              if (flight.return_confirmation || flight.confirmation) {
                returnFlightDescription += `Confirmation: ${flight.return_confirmation || flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                returnFlightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: returnStart,
                end: returnEnd,
                summary: `✈️ RETURN: ${flight.return_name || `${returnDepartureLocation} → ${returnArrivalLocation}`}`,
                location: returnDepartureLocation,
                description: returnFlightDescription,
                uid: `${returnEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });

              const returnInfo = {
                type: 'flight_return',
                title: `✈️ RETURN: ${flight.return_name || `${returnDepartureLocation} → ${returnArrivalLocation}`}`,
                start: returnStart.toISOString(),
                end: returnEnd.toISOString(),
                location: returnDepartureLocation,
                airline: flight.return_airline || flight.departure_airline,
                flightNumber: flight.return_flightnumber,
                confirmation: flight.return_confirmation || flight.confirmation,
                mainEvent: title,
                duration: `${Math.round((returnEnd - returnStart) / (1000 * 60))} minutes`
              };

              eventSummary.push(returnInfo);
            }
          });
        }
      }
    });

    // Return both JSON summary and option to download .ics
    if (req.query.format === 'ics') {
      res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="rehearsal-test.ics"');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.send(calendar.toString());
    } else {
      res.json({
        message: 'Rehearsal test completed successfully',
        totalEvents: eventSummary.filter(e => e.type === 'main_event').length,
        totalRehearsals: eventSummary.filter(e => e.type === 'rehearsal').length,
        totalFlights: eventSummary.filter(e => e.type.startsWith('flight_')).length,
        totalDepartureFlights: eventSummary.filter(e => e.type === 'flight_departure').length,
        totalReturnFlights: eventSummary.filter(e => e.type === 'flight_return').length,
        events: eventSummary,
        downloadIcs: `${req.protocol}://${req.get('host')}/test/rehearsals?format=ics`
      });
    }

  } catch (error) {
    console.error('Error testing rehearsals:', error);
    res.status(500).json({ error: error.message });
  }
});

// Test Andrew's assignment lookup
app.get('/debug/test-assignment/:assignmentId', async (req, res) => {
  const { assignmentId } = req.params;
  
  try {
    const assignmentPage = await notion.pages.retrieve({ page_id: assignmentId });
    
    res.json({
      assignmentId,
      assignmentPage: {
        id: assignmentPage.id,
        properties: Object.keys(assignmentPage.properties),
        personnel: assignmentPage.properties?.['Personnel']?.relation,
        personnelId: assignmentPage.properties?.['Personnel']?.relation?.[0]?.id,
        position: assignmentPage.properties?.['Position']?.select?.name
      }
    });
  } catch (error) {
    res.status(500).json({ 
      assignmentId,
      error: error.message 
    });
  }
});

// Calendar for specific person
app.get('/calendar/:personId', async (req, res) => {
  let { personId } = req.params;
  
  // Convert personId to proper UUID format if it doesn't have hyphens
  if (personId.length === 32 && !personId.includes('-')) {
    personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }
  
  try {
    // Get the person's record and their Calendar Feed JSON
    const person = await notion.pages.retrieve({ page_id: personId });
    const calendarFeedJson = person.properties?.['Calendar Feed JSON']?.formula?.string || 
                             person.properties?.['Calendar Feed JSON']?.rich_text?.[0]?.plain_text;
    
    if (!calendarFeedJson) {
      return res.status(404).json({ 
        error: 'No calendar feed data found for this person - formula may be empty',
        personId,
        personName: person.properties?.['Full Name']?.formula?.string || person.properties?.['Nickname']?.title?.[0]?.plain_text,
        propertyType: person.properties?.['Calendar Feed JSON']?.type,
        propertyValue: person.properties?.['Calendar Feed JSON']
      });
    }
    
    // Parse the calendar feed JSON
    let calendarData;
    try {
      // Clean up common JSON issues like unquoted dollar amounts
      const cleanedJson = calendarFeedJson.replace(/:\$(\d+)/g, ':"$$$1"');
      calendarData = JSON.parse(cleanedJson);
    } catch (parseError) {
      return res.status(400).json({ 
        error: 'Invalid JSON in Calendar Feed JSON property',
        personId,
        parseError: parseError.message,
        originalJson: calendarFeedJson.substring(0, 500) + '...' // First 500 chars for debugging
      });
    }
    
    // Handle different JSON structures
    let events = [];
    if (Array.isArray(calendarData)) {
      events = calendarData; // JSON is directly an array
    } else if (calendarData.events) {
      events = calendarData.events; // JSON has events property
    } else {
      events = []; // No recognizable structure
    }
    
    const response = { results: events };
    
    const calendar = ical({ 
      name: 'Downbeat Events',
      timezone: 'America/Los_Angeles'
    });
    
    response.results.forEach((event, index) => {
      // Handle the specific JSON format from your sample
      const title = event.event_name;
      const startDate = event.event_start;
      const endDate = event.event_end;
      const venue = event.venue;
      const venueAddress = event.venue_address;
      const band = event.band;
      const eventId = `event-${index}-${Date.now()}`;
      
      // Create location string from venue and address
      const location = venueAddress ? `${venue}, ${venueAddress}` : venue;
      
      if (startDate && title) {
        // Fix date format by replacing 'T' quotes with actual T
        const cleanStartDate = startDate.replace(/[']/g, '');
        const cleanEndDate = endDate ? endDate.replace(/[']/g, '') : cleanStartDate;
        
        // Build position assignments and pay information
        let positionInfo = '';
        if (event.payroll && event.payroll.length > 0) {
          positionInfo = event.payroll.map(payrollItem => {
            // Handle pay that might already have $ symbol or be just a number
            const payAmount = typeof payrollItem.pay_total === 'string' ? 
              (payrollItem.pay_total.startsWith('$') ? payrollItem.pay_total : `$${payrollItem.pay_total}`) : 
              `$${payrollItem.pay_total}`;
            return `Position: ${payrollItem.position}\nPosition Assignments:\n${payrollItem.assignment} - ${payAmount}`;
          }).join('\n\n');
        }
        
        // Build description starting with position info, then general_info
        let eventDescription = '';
        
        // Add position info first if available
        if (positionInfo) {
          eventDescription = positionInfo;
        }
        
        // Add general_info after position info, or fall back to band/venue format
        if (event.general_info) {
          const generalInfo = event.general_info
            .replace(/\r\n/g, '\n')  // Normalize line endings
            .replace(/\r/g, '\n')    // Normalize line endings
            .trim(); // Remove leading/trailing whitespace
          
          if (eventDescription) {
            eventDescription += '\n\n' + generalInfo;
          } else {
            eventDescription = generalInfo;
          }
        } else {
          const fallbackInfo = `Band: ${band}\nVenue: ${venue}`;
          if (eventDescription) {
            eventDescription += '\n\n' + fallbackInfo;
          } else {
            eventDescription = fallbackInfo;
          }
        }

        calendar.createEvent({
          start: new Date(cleanStartDate),
          end: new Date(cleanEndDate),
          summary: `${title} (${band})`,
          location: location || '',
          description: eventDescription,
          uid: `${eventId}@downbeat.agency`,
          url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
        });

        // Add rehearsal events if they exist
        if (event.rehearsals && Array.isArray(event.rehearsals)) {
          event.rehearsals.forEach((rehearsal, rehearsalIndex) => {
            if (rehearsal.rehearsal_time) {
              let rehearsalStart, rehearsalEnd;
              
              // Handle different rehearsal_time formats
              const rehearsalTimeStr = rehearsal.rehearsal_time.replace(/[']/g, '');
              
              if (rehearsalTimeStr.includes('→')) {
                // Format: "@Month DD, YYYY H:MM AM/PM (TZ) → H:MM AM/PM"
                const match = rehearsalTimeStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s+→\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
                if (match) {
                  const dateStr = match[1].trim(); // "September 11, 2025"
                  const startTimeStr = match[2].trim(); // "10:00 AM"
                  const endTimeStr = match[3].trim(); // "12:00 PM"
                  
                  // Parse the date and times
                  rehearsalStart = new Date(`${dateStr} ${startTimeStr}`);
                  rehearsalEnd = new Date(`${dateStr} ${endTimeStr}`);
                  
                  // Ensure valid dates
                  if (isNaN(rehearsalStart.getTime()) || isNaN(rehearsalEnd.getTime())) {
                    // Fallback to 3 hours if parsing fails
                    rehearsalStart = new Date(rehearsalTimeStr);
                    rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000);
                  }
                } else {
                  // Fallback if regex doesn't match
                  rehearsalStart = new Date(rehearsalTimeStr);
                  rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000);
                }
              } else if (rehearsalTimeStr.includes('/')) {
                // Format: "start/end" (e.g., "2025-09-11T10:00:00-07:00/2025-09-11T13:00:00-07:00")
                const [startStr, endStr] = rehearsalTimeStr.split('/');
                rehearsalStart = new Date(startStr.trim());
                rehearsalEnd = new Date(endStr.trim());
              } else if (rehearsal.rehearsal_start && rehearsal.rehearsal_end) {
                // Separate start/end fields
                rehearsalStart = new Date(rehearsal.rehearsal_start.replace(/[']/g, ''));
                rehearsalEnd = new Date(rehearsal.rehearsal_end.replace(/[']/g, ''));
              } else if (rehearsal.rehearsal_time && rehearsal.rehearsal_end) {
                // Start time in rehearsal_time, end time separate
                rehearsalStart = new Date(rehearsalTimeStr);
                rehearsalEnd = new Date(rehearsal.rehearsal_end.replace(/[']/g, ''));
              } else {
                // Only start time provided, default to 3 hours
                rehearsalStart = new Date(rehearsalTimeStr);
                rehearsalEnd = new Date(rehearsalStart.getTime() + 3 * 60 * 60 * 1000); // Add 3 hours
              }
              
              const rehearsalEventId = `rehearsal-${index}-${rehearsalIndex}-${Date.now()}`;
              const rehearsalLocationName = rehearsal.rehearsal_location || 'TBD';
              const rehearsalAddress = rehearsal.rehearsal_address || '';
              
              // Create full location string (name + address)
              let fullRehearsalLocation = rehearsalLocationName;
              if (rehearsalAddress) {
                fullRehearsalLocation = `${rehearsalLocationName}, ${rehearsalAddress}`;
              }
              
              calendar.createEvent({
                start: rehearsalStart,
                end: rehearsalEnd,
                summary: `REHEARSAL: ${title} (${band})`,
                location: fullRehearsalLocation,
                description: `Rehearsal for: ${title}\nBand: ${band}\nMain Event Venue: ${venue}`,
                uid: `${rehearsalEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });
            }
          });
        }

        // Add flight events if they exist
        if (event.flights && Array.isArray(event.flights)) {
          event.flights.forEach((flight, flightIndex) => {
            // Process departure flight
            if (flight.departure_name || flight.departure_airline) {
              // Use actual flight times if available, otherwise use placeholder times
              let departureStart, departureEnd;
              
              if (flight.departure_time) {
                // Use real departure time from Notion
                departureStart = new Date(flight.departure_time);
                departureEnd = flight.departure_arrival_time ? 
                  new Date(flight.departure_arrival_time) : 
                  new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                departureStart = new Date(new Date(event.event_date).getTime() - 24 * 60 * 60 * 1000); // 1 day before event
                departureStart.setHours(10, 0, 0, 0); // Set to 10:00 AM
                departureEnd = new Date(departureStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const departureEventId = `flight-departure-${index}-${flightIndex}-${Date.now()}`;
              
              // Create departure airport location
              const departureLocation = flight.departure_from_city ? 
                `${flight.departure_from} (${flight.departure_from_city.replace('@', '')})` : 
                flight.departure_from;
              
              const arrivalLocation = flight.departure_to_city ? 
                `${flight.departure_to} (${flight.departure_to_city.replace('@', '')})` : 
                flight.departure_to;
              
              // Build flight description
              let flightDescription = `Flight for: ${title}\nBand: ${band}\n`;
              flightDescription += `Airline: ${flight.departure_airline || 'TBD'}\n`;
              flightDescription += `Flight: ${flight.departure_flightnumber || 'TBD'}\n`;
              flightDescription += `From: ${departureLocation}\n`;
              flightDescription += `To: ${arrivalLocation}\n`;
              if (flight.confirmation) {
                flightDescription += `Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                flightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: departureStart,
                end: departureEnd,
                summary: `✈️ FLIGHT: ${flight.departure_name || `${departureLocation} → ${arrivalLocation}`}`,
                location: departureLocation,
                description: flightDescription,
                uid: `${departureEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });
            }

            // Process return flight
            if (flight.return_name || flight.return_airline) {
              // Use actual return flight times if available, otherwise use placeholder times
              let returnStart, returnEnd;
              
              if (flight.return_time) {
                // Use real return time from Notion
                returnStart = new Date(flight.return_time);
                returnEnd = flight.return_arrival_time ? 
                  new Date(flight.return_arrival_time) : 
                  new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour default if no arrival time
              } else {
                // Fallback to placeholder timing
                returnStart = new Date(new Date(event.event_date).getTime() + 24 * 60 * 60 * 1000); // 1 day after event
                returnStart.setHours(14, 0, 0, 0); // Set to 2:00 PM
                returnEnd = new Date(returnStart.getTime() + 3 * 60 * 60 * 1000); // 3 hour duration
              }
              
              const returnEventId = `flight-return-${index}-${flightIndex}-${Date.now()}`;
              
              // Create return airport location
              const returnDepartureLocation = flight.return_from_city ? 
                `${flight.return_from} (${flight.return_from_city.replace('@', '')})` : 
                flight.return_from;
              
              const returnArrivalLocation = flight.return_to_city ? 
                `${flight.return_to} (${flight.return_to_city.replace('@', '')})` : 
                flight.return_to;
              
              // Build return flight description
              let returnFlightDescription = `Return flight for: ${title}\nBand: ${band}\n`;
              returnFlightDescription += `Airline: ${flight.return_airline || flight.departure_airline || 'TBD'}\n`;
              returnFlightDescription += `Flight: ${flight.return_flightnumber || 'TBD'}\n`;
              returnFlightDescription += `From: ${returnDepartureLocation}\n`;
              returnFlightDescription += `To: ${returnArrivalLocation}\n`;
              if (flight.return_confirmation || flight.confirmation) {
                returnFlightDescription += `Confirmation: ${flight.return_confirmation || flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                returnFlightDescription += `Status: ${flight.flight_status}`;
              }
              
              calendar.createEvent({
                start: returnStart,
                end: returnEnd,
                summary: `✈️ RETURN: ${flight.return_name || `${returnDepartureLocation} → ${returnArrivalLocation}`}`,
                location: returnDepartureLocation,
                description: returnFlightDescription,
                uid: `${returnEventId}@downbeat.agency`,
                url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
              });
            }
          });
        }
      }
    });
    
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', 'inline; filename="calendar.ics"');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    
    res.send(calendar.toString());
    
  } catch (error) {
    console.error('Error generating calendar:', error);
    res.status(500).send('Error generating calendar');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});