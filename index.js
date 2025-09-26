import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ auth: process.env.NOTION_API_KEY });

const EVENTS_DB = '3dec3113-f747-49db-b666-8ba1f06c1d3e';
const PERSONNEL_DB = 'f8044a3d-6c88-4579-bbe0-2d15de3448be';

// Home page - List all personnel
app.get('/', async (req, res) => {
  try {
    const response = await notion.databases.query({
      database_id: PERSONNEL_DB,
      sorts: [{ property: 'Full Name', direction: 'ascending' }]
    });
    
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
              
              return `
                <div class="person">
                  <span class="name">${name}</span>
                  <a href="/calendar/${personId}" class="button">Get Calendar</a>
                </div>
              `;
            }).join('')}
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

// Calendar for specific person
app.get('/calendar/:personId', async (req, res) => {
  const { personId } = req.params;
  
  try {
    const response = await notion.databases.query({
      database_id: EVENTS_DB,
      filter: {
        property: 'Payroll Personnel',
        relation: { contains: personId }
      },
      sorts: [{ property: 'Event Date', direction: 'ascending' }]
    });
    
    const calendar = ical({ 
      name: 'Downbeat Events',
      timezone: 'America/Los_Angeles'
    });
    
    response.results.forEach(event => {
      const props = event.properties;
      const eventDate = props['Event Date']?.date;
      const title = props.Event?.title?.[0]?.plain_text;
      const location = props['Location (Print)']?.rich_text?.[0]?.plain_text;
      const eventType = props['Event Type']?.select?.name;
      
      if (eventDate && title) {
        calendar.createEvent({
          start: new Date(eventDate.start),
          end: eventDate.end ? new Date(eventDate.end) : new Date(eventDate.start),
          summary: title,
          location: location || '',
          description: `Type: ${eventType || 'Event'}\n\nView in Notion: ${event.url}`,
          uid: `${event.id}@downbeat.agency`,
          url: event.url
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