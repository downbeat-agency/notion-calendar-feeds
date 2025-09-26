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
      calendarData = JSON.parse(calendarFeedJson);
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
      calendarData = JSON.parse(calendarFeedJson);
    } catch (parseError) {
      return res.status(400).json({ 
        error: 'Invalid JSON in Calendar Feed JSON property',
        personId,
        parseError: parseError.message
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
        
        calendar.createEvent({
          start: new Date(cleanStartDate),
          end: new Date(cleanEndDate),
          summary: `${title} (${band})`,
          location: location || '',
          description: `Band: ${band}\nVenue: ${venue}`,
          uid: `${eventId}@downbeat.agency`,
          url: event.notion_url || `https://www.notion.so/downbeat/Events-3dec3113f74749dbb6668ba1f06c1d3e`
        });
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