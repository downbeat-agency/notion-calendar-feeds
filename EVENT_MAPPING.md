# Calendar Feed Event Processing Documentation

## Overview
The server processes JSON data from Notion's "Calendar Feed JSON" formula property and converts it into calendar events. Each JSON object can generate multiple calendar events (main event + flights + rehearsals).

## Event Detection Rules

### 1. **Main Events** (Weddings/Gigs)
**Triggers:** Object has `event_name` AND `event_start`

**Mapping:**
```javascript
{
  type: 'main_event',
  title: event.event_name,              // "Santa Monica Wedding"
  start: event.event_start,             // "2025-08-30T12:00:00-07:00"
  end: event.event_end,                 // "2025-08-31T01:00:00-07:00"
  description: event.general_info,       // Load-in info, dress code, etc.
  location: event.venue_address || event.venue,  // Full address preferred
  url: event.notion_url                 // Link back to Notion
}
```

### 2. **Flight Events** 
**Triggers:** Object has `flights` array with flight objects

**Departure Flight Mapping:**
```javascript
// Requires: flight.departure_time AND flight.departure_name
{
  type: 'flight_departure',
  title: "✈️ Flight to HNL (Band)",     // From flight.departure_name
  start: flight.departure_time,         // "2025-09-05T08:48:00-07:00"
  end: flight.departure_arrival_time,   // "2025-09-05T11:45:00-07:00"
  description: "Confirmation: HEOOAO\nAirline: American Airlines\nFlight: AA 31",
  location: flight.departure_from || "Airport",
  confirmation: flight.confirmation,
  mainEvent: event.event_name           // Links back to main event
}
```

**Return Flight Mapping:**
```javascript
// Requires: flight.return_time AND flight.return_name
{
  type: 'flight_return',
  title: "✈️ Flight Return to LAX (Band)",  // From flight.return_name
  start: flight.return_time,               // "2025-09-07T15:26:00-07:00"
  end: flight.return_arrival_time,         // "2025-09-07T23:58:00-07:00"
  description: "Confirmation: HEOOAO\nFlight: AA 164",
  location: flight.return_from || "Airport",
  mainEvent: event.event_name
}
```

### 3. **Rehearsal Events**
**Triggers:** Object has `rehearsals` array with rehearsal objects

**Rehearsal Mapping:**
```javascript
// Requires: rehearsal.rehearsal_time (not null)
{
  type: 'rehearsal',
  title: "🎵 Rehearsal - Kapolei Wedding",  // Links to main event
  start: rehearsal.rehearsal_time,          // "2025-09-03T14:00:00-07:00"
  end: rehearsal.rehearsal_time,            // Same as start (no duration)
  description: "Rehearsal for Kapolei Wedding",
  location: rehearsal.rehearsal_address || rehearsal.rehearsal_location || "TBD",
  mainEvent: event.event_name
}
```

## Required JSON Structure

Your Notion formula should output an array of objects like this:

```javascript
[
  {
    // MAIN EVENT (Required)
    "event_name": "Toronto Wedding",              // REQUIRED
    "event_start": "2025-09-13T12:00:00-07:00",  // REQUIRED
    "event_end": "2025-09-14T02:00:00-07:00",    // Optional
    "venue": "Langdon Hall",                     // Optional
    "venue_address": "1 Langdon Dr, Cambridge",  // Optional (preferred)
    "general_info": "Parking info, dress code",  // Optional
    "notion_url": "https://www.notion.so/...",   // Optional
    "band": "Soultones",                         // Optional
    
    // FLIGHTS ARRAY (Optional)
    "flights": [
      {
        "confirmation": "BW3E7Y",
        "flight_status": "Booked",
        "flight_type": "One-Way",
        
        // DEPARTURE (Creates separate flight event if present)
        "departure_name": "Flight to LAX (Josh)",      // REQUIRED for departure event
        "departure_time": "2025-09-16T18:35:00-07:00", // REQUIRED for departure event
        "departure_arrival_time": "2025-09-16T20:53:00-07:00",
        "departure_airline": "Air Canada",
        "departure_flightnumber": "AC 795",
        "departure_from": "Toronto Airport",
        
        // RETURN (Creates separate return flight event if present)
        "return_name": "Flight Return to LAX",         // REQUIRED for return event
        "return_time": "2025-09-07T15:26:00-07:00",   // REQUIRED for return event
        "return_arrival_time": "2025-09-07T23:58:00-07:00",
        "return_airline": "American Airlines",
        "return_flightnumber": "AA 164",
        "return_from": "HNL Airport"
      }
    ],
    
    // REHEARSALS ARRAY (Optional)
    "rehearsals": [
      {
        "rehearsal_time": "2025-09-03T14:00:00-07:00",  // REQUIRED (null = ignored)
        "rehearsal_location": "Downbeat HQ",             // Optional
        "rehearsal_address": "123 W Bellevue Dr"         // Optional (preferred)
      }
    ],
    
    // OTHER OPTIONAL FIELDS
    "payroll": [...],      // Ignored by calendar (for reference only)
    "pay_total": 2425      // Ignored by calendar (for reference only)
  }
]
```

## API Response Structure

```javascript
{
  "personName": "Joshua Paul Sipfle",
  "totalMainEvents": 2,           // Count of main JSON objects
  "totalCalendarEvents": 6,       // Count of all calendar events generated
  "breakdown": {
    "mainEvents": 2,              // Wedding/gig events
    "flights": 3,                 // Flight departure + return events  
    "rehearsals": 1               // Rehearsal events
  },
  "events": [
    // Array of all calendar events (main + flights + rehearsals)
  ]
}
```

## Event Types Generated

1. **Main Events**: `type: 'main_event'` - One per JSON object
2. **Flight Departures**: `type: 'flight_departure'` - One per flight with departure info
3. **Flight Returns**: `type: 'flight_return'` - One per flight with return info  
4. **Rehearsals**: `type: 'rehearsal'` - One per rehearsal with valid time

## Calendar Integration

- **ICS Format**: `GET /calendar/:personId?format=ics` - All events in calendar format
- **JSON Format**: `GET /calendar/:personId` - Structured data with event breakdown
- **Event Titles**: Include emojis (✈️ for flights, 🎵 for rehearsals) for easy identification
- **Descriptions**: Include confirmation numbers, flight details, etc.
- **Links**: Main events link back to Notion via `notion_url`

## Optimization Notes

To ensure your Notion formula works within API timeout limits:
- **Limit events**: Process max 5-10 events at a time
- **Reduce complexity**: Avoid nested loops and complex string operations  
- **Test incrementally**: Start simple and add complexity gradually
- **Use date filters**: Limit to recent/upcoming events only

## Testing

Use the debug endpoint to verify data:
```bash
curl "https://notion-calendar-feeds-production.up.railway.app/debug/simple-test/PERSON_ID"
```
