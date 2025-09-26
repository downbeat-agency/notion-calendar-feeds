import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';

const app = express();
const port = process.env.PORT || 3000;
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
    const response = await notion.databases.query({
      database_id: EVENTS_DB,
      page_size: 50,
      sorts: [{ property: 'Event Date', direction: 'descending' }]
    });
    
    const eventDebug = response.results.map(event => {
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
      totalEventsInDB: response.results.length,
      events: eventDebug,
      hasMore: response.has_more
    });
    
  } catch (error) {
    console.error('Error debugging events:', error);
    res.status(500).json({ error: error.message });
  }
});

// Debug calendar data for specific person  
app.get('/debug/calendar/:personId', async (req, res) => {
  const { personId } = req.params;
  
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
        jsonStructure: {
          isArray: Array.isArray(calendarData),
          hasEventsProperty: !!calendarData.events,
          topLevelKeys: Object.keys(calendarData).slice(0, 10),
          firstEventSample: events[0] || null
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
            return `Position: ${payrollItem.position.replace(/:/g, ' -')}\nPosition Assignments:\n${payrollItem.assignment} - ${payAmount}`;
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
                // Format: "@Month DD, YYYY H:MM AM/PM → H:MM AM/PM"
                const match = rehearsalTimeStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))\s+→\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
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
  const { personId } = req.params;
  
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
            return `Position: ${payrollItem.position.replace(/:/g, ' -')}\nPosition Assignments:\n${payrollItem.assignment} - ${payAmount}`;
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
                // Format: "@Month DD, YYYY H:MM AM/PM → H:MM AM/PM"
                const match = rehearsalTimeStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))\s+→\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
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