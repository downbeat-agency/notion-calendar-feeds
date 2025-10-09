# Calendar Feed Event Processing Documentation

## Overview
The server processes calendar data from two sources:
1. **Legacy**: Notion's "Calendar Feed JSON" formula property (single JSON array)
2. **New**: Notion's "Calendar Data" database with separate formula fields for each event type

Each data source can generate multiple calendar events (main events + flights + rehearsals + hotels + transportation + team calendar).

## Data Sources

### Legacy Source: "Calendar Feed JSON" Formula
- **Location**: Personnel database → "Calendar Feed JSON" property
- **Format**: Single JSON array with all event data
- **Usage**: `GET /calendar/:personId` (default)

### New Source: "Calendar Data" Database  
- **Location**: Separate "Calendar Data" database
- **Format**: Individual formula fields for each event type
- **Usage**: `GET /calendar/:personId?source=new`
- **Fields**:
  - `Events` - Main events (weddings/gigs)
  - `Flights` - Flight information
  - `Rehearsals` - Rehearsal schedules
  - `Hotels` - Hotel bookings
  - `Transportation` - Ground transport
  - `Team Calendar` - Office days and team events

## Event Detection Rules

### 1. **Main Events** (Weddings/Gigs)
**Triggers:** Object has `event_name` AND `event_date` (new) OR `event_start` (legacy)

**Mapping:**
```javascript
{
  type: 'main_event',
  title: event.event_name,              // "Santa Monica Wedding"
  start: event.event_date || event.event_start,  // "2025-08-30T12:00:00-07:00"
  end: event.event_date || event.event_end,      // "2025-08-31T01:00:00-07:00"
  description: event.general_info,       // Load-in info, dress code, etc.
  location: event.venue_address || event.venue,  // Full address preferred
  url: event.notion_url,                // Link back to Notion
  band: event.band,                     // Band name
  calltime: event.calltime,             // Call time (converted to local time)
  pay_total: event.pay_total,           // Payment amount
  position: event.position,             // Musician position
  assignments: event.assignments        // Additional assignments
}
```

### 2. **Flight Events** 
**Triggers:** `flights` array with flight objects (legacy) OR top-level `Flights` field (new)

**Departure Flight Mapping:**
```javascript
// Requires: flight.departure_time AND flight.departure_name
{
  type: 'flight_departure',
  title: "✈️ Flight to HNL (Band)",     // From flight.departure_name
  start: flight.departure_time,         // "2025-09-05T08:48:00-07:00"
  end: flight.departure_arrival_time,   // "2025-09-05T11:45:00-07:00"
  description: "Confirmation: HEOOAO\nAirline: American Airlines\nFlight: AA 31",
  location: flight.departure_airport || flight.departure_from || "Airport",
  confirmation: flight.confirmation,
  airline: flight.departure_airline,
  flightNumber: flight.departure_flightnumber,
  mainEvent: event.event_name || ""     // Links back to main event (empty for top-level)
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
  location: flight.return_airport || flight.return_from || "Airport",
  confirmation: flight.confirmation,
  airline: flight.return_airline,
  flightNumber: flight.return_flightnumber,
  mainEvent: event.event_name || ""       // Links back to main event (empty for top-level)
}
```

### 3. **Rehearsal Events**
**Triggers:** `rehearsals` array with rehearsal objects (legacy) OR top-level `Rehearsals` field (new)

**Rehearsal Mapping:**
```javascript
// Requires: rehearsal.rehearsal_time (not null)
{
  type: 'rehearsal',
  title: "🎤 Rehearsal",                    // Fixed title
  start: rehearsal.rehearsal_time,          // "2025-09-03T14:00:00-07:00"
  end: rehearsal.rehearsal_time,            // Same as start (no duration)
  description: "Rehearsal\n\nBand Personnel:\n" + rehearsal.rehearsal_band,
  location: rehearsal.rehearsal_address || rehearsal.rehearsal_location || "TBD",
  url: rehearsal.rehearsal_pco,            // PCO link
  mainEvent: event.event_name || ""        // Links back to main event (empty for top-level)
}
```

### 4. **Hotel Events**
**Triggers:** Top-level `Hotels` field (new source only)

**Hotel Mapping:**
```javascript
// Requires: hotel.dates_booked
{
  type: 'hotel',
  title: "🏨 " + (hotel.hotel_name || hotel.title || "Hotel"),
  start: hotel.dates_booked,               // "2025-09-20T23:00:00+00:00/2025-09-21T18:00:00+00:00"
  end: hotel.dates_booked,                 // Same as start (date range)
  description: "Hotel Stay\nConfirmation: " + hotel.confirmation + "\nPhone: " + hotel.hotel_phone + "\n\nNames on Reservation:\n" + hotel.names_on_reservation + "\nBooked Under: " + hotel.booked_under,
  location: hotel.hotel_address || hotel.hotel_name || "Hotel",
  url: hotel.hotel_google_maps || hotel.hotel_apple_maps || "",
  confirmation: hotel.confirmation,
  hotelName: hotel.hotel_name,
  mainEvent: ""                            // Not tied to specific main event
}
```

### 5. **Transportation Events**
**Triggers:** Top-level `Transportation` field (new source only)

**Transportation Mapping:**
```javascript
// Requires: transport.start
{
  type: 'ground_transport_pickup' | 'ground_transport_dropoff' | 'ground_transport_meeting' | 'ground_transport',
  title: "🚙 " + (transport.title || "Ground Transport"),
  start: transport.start,                  // "2025-09-20T14:00:00+00:00"
  end: transport.end || (start + 30 minutes),  // Default 30-minute duration
  description: transport.description,      // Driver info, passengers, etc.
  location: transport.location || "",
  mainEvent: ""                            // Not tied to specific main event
}
```

### 6. **Team Calendar Events**
**Triggers:** Top-level `Team Calendar` field (new source only)

**Team Calendar Mapping:**
```javascript
// Requires: teamEvent.date
{
  type: 'team_calendar',
  title: "📅 " + (teamEvent.title || "Team Event"),
  start: teamEvent.date,                   // "2025-09-10T10:30:00-07:00/2025-09-10T18:30:00-07:00"
  end: teamEvent.date,                     // Same as start (date range)
  description: teamEvent.notes || "",
  location: teamEvent.address || "",       // Office address or location
  url: teamEvent.notion_link || "",
  mainEvent: ""                            // Not tied to specific main event
}
```

## Required JSON Structure

### Legacy Format: "Calendar Feed JSON" Formula
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

### New Format: "Calendar Data" Database
The new database uses separate formula fields for each event type:

```javascript
{
  "Events": "[{\"event_name\":\" Wedding\",\"notion_url\":\"https://www.notion.so/...\",\"event_date\":\"2025-08-26T15:30:00+00:00/2025-09-14T06:00:00+00:00\",\"band\":\"Gold Standard\",\"calltime\":\"2025-08-26T15:30:00+00:00\",\"gear_checklist\":\"\",\"general_info\":\"Parking and Load In:...\",\"venue\":\"Casa Del Mar\",\"venue_address\":\"1910 Ocean Way, Santa Monica, CA 90405\",\"pay_total\":800,\"position\":\"Drums\",\"assignments\":\"Base + Rehearsal\"}]",
  
  "Flights": "[{\"confirmation\":\"HWSV8Y\",\"departure_name\":\"Flight to JFK (Diego)\",\"departure_airline\":\"Delta\",\"departure_flightnumber\":\"DL 915\",\"departure_time\":\"2025-10-10T06:55:00+00:00/2025-10-10T15:30:00+00:00\",\"departure_airport\":\"1 World Way, Los Angeles, CA 90045\",\"return_name\":\"Flight Return to LAX (Diego)\",\"return_airline\":\"Delta\",\"return_airport\":\"JFK Access Rd, Jamaica, NY 11430\",\"return_flightnumber\":\"DL 773\",\"return_time\":\"2025-10-12T16:55:00+00:00/2025-10-12T20:02:00+00:00\"}]",
  
  "Rehearsals": "[{\"rehearsal_time\":\"2025-09-11T17:00:00+00:00/2025-09-11T19:00:00+00:00\",\"rehearsal_pco\":\"https://services.planningcenteronline.com/plans/81859026\",\"rehearsal_band\":\"Bass - Eric  🟢\\nDrums - Diego  🟢\\nGuitar - Silas  🟢\\nKeys - Kevin  🟢\\nVox 1 - Revel  🟢\\nVox 2 - Dani  🟢\\nVox 3 - Joe  🟢\\nVox 4 - Ayo  🟢\",\"rehearsal_location\":\"\",\"rehearsal_address\":\"\"}]",
  
  "Hotels": "[{\"title\":\"Hotel -  (Band)\",\"hotel_name\":\"Hilton Garden Inn Sonoma County Airport\",\"hotel_phone\":\"(707) 545-0444\",\"hotel_address\":\"417 Aviation Blvd, Santa Rosa, CA 95403\",\"confirmation\":\"3291242890\",\"names_on_reservation\":\"Jackie,Eric,Joakim,Dave,Payson,Byron,Diego,Gabe,Michael\",\"booked_under\":\"Diego\",\"dates_booked\":\"2025-09-20T23:00:00+00:00/2025-09-21T18:00:00+00:00\"}]",
  
  "Transportation": "[{\"title\":\"MEET UP: Band Sprinter ( Wedding)\",\"start\":\"2025-09-20T14:00:00+00:00\",\"end\":\"2025-09-20T14:00:00+00:00\",\"location\":\"149 N Halstead St, Pasadena, CA 91107\",\"description\":\"Driver: Diego De la Rosa\\nPassenger: Eric England,Diego De la Rosa,Gabriel Rudner,Michael Czaja,Joakim Toftgaard,Michael Campagna,Jacquelyn Foster\\nMeet Up Info: Meetup Location: Sierra Madre Villa,149 N Halstead St, Pasadena, CA 91107,\\nDriver Info:\\nDiego - (626) 991-4302,\\nMeetup Notes: Make sure you bring a coffee for Diego\",\"type\":\"ground_transport_meeting\"}]",
  
  "Team Calendar": "[{\"title\":\"Office\",\"address\":\"123 W Bellevue Dr Ste 4, Pasadena CA 91105\",\"date\":\"2025-09-11T10:30:00-07:00/2025-09-11T18:30:00-07:00\",\"notes\":\"\",\"notion_link\":\"https://www.notion.so/17839e4a65a9801f8ae5c1d36810bebc\"}]"
}
```

## API Response Structure

```javascript
{
  "personName": "Diego De la Rosa",
  "totalMainEvents": 30,          // Count of main events
  "totalCalendarEvents": 128,     // Count of all calendar events generated
  "dataSource": "new",            // "new" or "old" (legacy)
  "breakdown": {
    "mainEvents": 30,             // Wedding/gig events
    "flights": 10,                // Flight departure + return events  
    "rehearsals": 16,             // Rehearsal events
    "hotels": 7,                  // Hotel bookings
    "groundTransport": 5,         // Ground transportation events
    "teamCalendar": 60            // Office days and team events
  },
  "events": [
    // Array of all calendar events (main + flights + rehearsals + hotels + transport + team)
  ]
}
```

## Event Types Generated

1. **Main Events**: `type: 'main_event'` - One per event in Events array
2. **Flight Departures**: `type: 'flight_departure'` - One per flight with departure info
3. **Flight Returns**: `type: 'flight_return'` - One per flight with return info  
4. **Rehearsals**: `type: 'rehearsal'` - One per rehearsal with valid time
5. **Hotels**: `type: 'hotel'` - One per hotel booking
6. **Transportation**: `type: 'ground_transport_pickup'|'ground_transport_dropoff'|'ground_transport_meeting'|'ground_transport'` - One per transport event
7. **Team Calendar**: `type: 'team_calendar'` - One per office day or team event

## Calendar Integration

- **Legacy Data Source**: `GET /calendar/:personId` - Uses "Calendar Feed JSON" formula
- **New Data Source**: `GET /calendar/:personId?source=new` - Uses "Calendar Data" database
- **ICS Format**: `GET /calendar/:personId?format=ics` - All events in calendar format
- **JSON Format**: `GET /calendar/:personId` - Structured data with event breakdown
- **Event Titles**: Include emojis (✈️ for flights, 🎤 for rehearsals, 🏨 for hotels, 🚙 for transport, 📅 for team calendar) for easy identification
- **Descriptions**: Include confirmation numbers, flight details, call times, etc.
- **Links**: Main events link back to Notion via `notion_url`
- **Call Time**: Automatically converted from UTC to America/Los_Angeles floating time

## Optimization Notes

To ensure your Notion formula works within API timeout limits:
- **Limit events**: Process max 5-10 events at a time
- **Reduce complexity**: Avoid nested loops and complex string operations  
- **Test incrementally**: Start simple and add complexity gradually
- **Use date filters**: Limit to recent/upcoming events only

## Testing

Use the debug endpoints to verify data:

**Legacy Data Source:**
```bash
curl "https://calendar.downbeat.agency/debug/simple-test/PERSON_ID"
```

**New Data Source:**
```bash
curl "https://calendar.downbeat.agency/debug/calendar-data/PERSON_ID"
```

**Calendar API:**
```bash
# Legacy source
curl "https://calendar.downbeat.agency/calendar/PERSON_ID"

# New source  
curl "https://calendar.downbeat.agency/calendar/PERSON_ID?source=new"

# ICS format
curl "https://calendar.downbeat.agency/calendar/PERSON_ID?format=ics"
```
