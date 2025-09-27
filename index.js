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
      calendarFeedLength: person.properties?.['Calendar Feed JSON']?.formula?.string?.length || 0
    };

    console.log('=== SIMPLE TEST DEBUG ===');
    console.log('Test Results:', JSON.stringify(testResults, null, 2));
    console.log('=== END SIMPLE TEST ===');

    res.json(testResults);
  } catch (error) {
    console.error('Simple test error:', error);
    res.status(500).json({ error: 'Error in simple test', details: error.message });
  }
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
    
    // Add comprehensive debugging
    console.log('=== API DEBUG INFO ===');
    console.log('PersonId:', personId);
    console.log('Person Full Name:', person.properties?.['Full Name']?.formula?.string);
    console.log('Raw Calendar Feed JSON length:', calendarFeedJson?.length || 0);
    console.log('Raw Calendar Feed JSON preview (first 500 chars):', calendarFeedJson?.substring(0, 500) || 'NULL');
    console.log('Available properties:', Object.keys(person.properties || {}));
    console.log('=== END DEBUG INFO ===');
    
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

    // Process all events into a flat array including main events, flights, and rehearsals
    const allCalendarEvents = [];
    
    events.forEach(event => {
      // Add main event
      if (event.event_name && event.event_start) {
        allCalendarEvents.push({
          type: 'main_event',
          title: event.event_name,
          start: event.event_start,
          end: event.event_end || event.event_start,
          description: event.general_info || '',
          location: event.venue_address || event.venue || '',
          url: event.notion_url || '',
          band: event.band || '',
          mainEvent: event.event_name
        });
      }

      // Add flight events
      if (event.flights && Array.isArray(event.flights)) {
        event.flights.forEach(flight => {
          // Departure flight
          if (flight.departure_time && flight.departure_name) {
            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: flight.departure_time,
              end: flight.departure_arrival_time || flight.departure_time,
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
            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: flight.return_time,
              end: flight.return_arrival_time || flight.return_time,
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
            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎵 Rehearsal - ${event.event_name}`,
              start: rehearsal.rehearsal_time,
              end: rehearsal.rehearsal_time, // Rehearsals typically don't have end times
              description: `Rehearsal for ${event.event_name}`,
              location: rehearsal.rehearsal_address || rehearsal.rehearsal_location || 'TBD',
              mainEvent: event.event_name
            });
          }
        });
      }
    });

    if (format === 'ics') {
      // Generate ICS calendar with all events
      const calendar = ical({ name: `${person.properties?.['Full Name']?.formula?.string || 'Personnel'} Calendar` });

      allCalendarEvents.forEach(event => {
        calendar.createEvent({
          start: new Date(event.start),
          end: new Date(event.end),
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || ''
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
        rehearsals: allCalendarEvents.filter(e => e.type === 'rehearsal').length
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
