import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ auth: process.env.NOTION_API_KEY });

// Use environment variable for Personnel database ID
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;

// Health check endpoint
app.get('/', (_req, res) => {
  res.json({
    status: 'Calendar Feed Server Running',
    endpoints: {
      calendar: '/calendar/:personId',
      ics: '/calendar/:personId?format=ics'
    }
  });
});

// Main calendar endpoint
app.get('/calendar/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    const format = req.query.format;

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
    
    // Debug: Log what API actually receives
    console.log('=== API DEBUG ===');
    console.log('Personnel DB ID:', PERSONNEL_DB);
    console.log('Person ID:', personId);
    console.log('Raw Calendar Feed JSON from API:', calendarFeedJson);
    console.log('Calendar Feed JSON length:', calendarFeedJson?.length);
    
    if (!calendarFeedJson) {
      console.log('Available properties:', Object.keys(person.properties || {}));
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

    if (format === 'ics') {
      // Generate ICS calendar
      const calendar = ical({ name: `${person.properties?.['Full Name']?.formula?.string || 'Personnel'} Calendar` });

      events.forEach(event => {
        if (event.event_name && event.event_start) {
        calendar.createEvent({
            start: new Date(event.event_start),
            end: new Date(event.event_end || event.event_start),
            summary: event.event_name,
            description: event.general_info || '',
            location: event.venue_address || event.venue || '',
            url: event.notion_url || ''
          });
        }
      });

      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="calendar.ics"');
      return res.send(calendar.toString());
    }

    // Return JSON format
    res.json({
      personName: person.properties?.['Full Name']?.formula?.string || 'Unknown',
      totalEvents: events.length,
      events
    });

  } catch (error) {
    console.error('Calendar generation error:', error);
    res.status(500).json({ error: 'Error generating calendar' });
  }
});

app.listen(port, () => {
  console.log(`Calendar feed server running on port ${port}`);
});
