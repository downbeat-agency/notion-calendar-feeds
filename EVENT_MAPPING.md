# Calendar Feed Event Processing Documentation

## Overview
The server processes calendar data from Notion's "Calendar Data" database with separate formula fields for each event type.

Each data source can generate multiple calendar events (main events + flights + layovers + rehearsals + hotels + transportation + team calendar).

## Data Source

### "Calendar Data" Database  
- **Location**: Separate "Calendar Data" database
- **Format**: Individual formula fields for each event type
- **Fields**:
  - `Events` - Main events (weddings/gigs) - **12 fields**
  - `Flights` - Flight information - **25 fields**
  - `Rehearsals` - Rehearsal schedules - **6 fields**
  - `Hotels` - Hotel bookings - **9 fields**
  - `Transportation` - Ground transport - **7 fields**
  - `Team Calendar` - Office days and team events - **5 fields**
- **Total**: 64 fields across 6 event types

## Event Detection Rules

### 1. **Main Events** (Weddings/Gigs)
**Triggers:** Object has `event_name` AND `event_date`

**Available Fields (12 total):**
- `event_name` - Event title (required)
- `event_date` - ISO 8601 date range (required)
- `notion_url` - Link back to Notion page
- `band` - Band name
- `event_personnel` - Formatted list of event personnel (all roles)
- `calltime` - Call time (ISO 8601)
- `gear_checklist` - Equipment checklist
- `general_info` - Load-in info, dress code, notes
- `venue` - Venue name
- `venue_address` - Full venue address
- `pay_total` - Payment amount
- `position` - Musician position
- `assignments` - Additional assignments

**Mapping:**
```javascript
{
  type: 'main_event',
  title: event.event_name,              // " Wedding"
  start: event.event_date,              // "2025-09-13T22:00:00+00:00/2025-09-14T06:00:00+00:00"
  end: event.event_date,                // Same as start (date range)
  description: payrollInfo + calltimeInfo + gearChecklistInfo + eventPersonnelInfo + notionUrlInfo + event.general_info,
  // Description includes:
  // - Position, Assignments, Pay (if present)
  // - Call Time (if present)
  // - Gear Checklist (if present)
  // - Event Personnel (if present) - formatted list of all roles
  // - Notion Link (if present)
  // - General Info (load-in, dress code, etc.)
  location: event.venue_address || event.venue,  // "1910 Ocean Way, Santa Monica, CA 90405"
  url: event.notion_url,                // "https://www.notion.so/13839e4a65a9804c8d66d0574a4acbf6"
  band: event.band,                     // "Gold Standard"
  pay_total: event.pay_total,           // 800
  position: event.position,             // "Drums"
  assignments: event.assignments        // "Base + Rehearsal"
}
```

### 2. **Flight Events** 
**Triggers:** Top-level `Flights` field

**Available Fields (25 total):**
- `confirmation` - Booking confirmation number
- `flight_url` - Link to Notion page for this flight
- `airport_arrival` - Arrival time recommendations
- `flight_status` - Status (Booked, Pending, etc.)
- `flight_type` - Type (Round Trip, One-Way, etc.)
- `departure_name` - Departure flight name (required for departure event)
- `departure_airline` - Departure airline
- `departure_flightnumber` - Departure flight number
- `departure_time` - Departure time as ISO 8601 date range (required)
- `departure_airport` - Departure airport address
- `departure_airport_name` - Departure airport name
- `departure_lo_flightnumber` - Departure layover flight number
- `departure_lo_time` - Departure layover time as ISO 8601 date range
- `departure_lo_from_airport` - Departure layover origin airport
- `departure_lo_to_airport` - Departure layover destination airport
- `return_name` - Return flight name (required for return event)
- `return_airline` - Return airline
- `return_airport` - Return airport address
- `return_airport_name` - Return airport name
- `return_flightnumber` - Return flight number
- `return_time` - Return time as ISO 8601 date range (required)
- `return_lo_flightnumber` - Return layover flight number
- `return_lo_time` - Return layover time as ISO 8601 date range
- `return_lo_from_airport` - Return layover origin airport
- `return_lo_to_airport` - Return layover destination airport

**Departure Flight Mapping:**
```javascript
// Requires: flight.departure_time AND flight.departure_name
{
  type: 'flight_departure',
  title: "✈️ Flight to JFK (Diego)",    // From flight.departure_name
  start: flight.departure_time,         // "2025-10-10T06:55:00+00:00/2025-10-10T15:30:00+00:00"
  end: flight.departure_time,           // Same as start (date range)
  description: "Confirmation: HWSV8Y\nAirline: Delta\nFlight: DL 915\nStatus: Booked\nType: Round Trip\n\nAirport Arrival: Domestic flights (within the U.S.) → Arrive 2 hours before departure.\n\nNotion Link: https://www.notion.so/26939e4a65a980f6839bd853232eaa52",
  location: flight.departure_airport,   // "1 World Way, Los Angeles, CA 90045"
  url: flight.flight_url,               // "https://www.notion.so/26939e4a65a980f6839bd853232eaa52"
  confirmation: flight.confirmation,    // "HWSV8Y"
  airline: flight.departure_airline,    // "Delta"
  flightNumber: flight.departure_flightnumber,  // "DL 915"
  flightStatus: flight.flight_status,   // "Booked"
  flightType: flight.flight_type,       // "Round Trip"
  airportArrival: flight.airport_arrival  // Arrival recommendations
}
```

**Return Flight Mapping:**
```javascript
// Requires: flight.return_time AND flight.return_name
{
  type: 'flight_return',
  title: "✈️ Flight Return to LAX (Diego)",  // From flight.return_name
  start: flight.return_time,                 // "2025-10-12T16:55:00+00:00/2025-10-12T20:02:00+00:00"
  end: flight.return_time,                   // Same as start (date range)
  description: "Confirmation: HWSV8Y\nAirline: Delta\nFlight: DL 773\nStatus: Booked\nType: Round Trip\n\nAirport Arrival: Domestic flights (within the U.S.) → Arrive 2 hours before departure.\n\nNotion Link: https://www.notion.so/26939e4a65a980f6839bd853232eaa52",
  location: flight.return_airport,           // "JFK Access Rd, Jamaica, NY 11430"
  url: flight.flight_url,                    // "https://www.notion.so/26939e4a65a980f6839bd853232eaa52"
  confirmation: flight.confirmation,         // "HWSV8Y"
  airline: flight.return_airline,            // "Delta"
  flightNumber: flight.return_flightnumber,  // "DL 773"
  flightStatus: flight.flight_status,        // "Booked"
  flightType: flight.flight_type,            // "Round Trip"
  airportArrival: flight.airport_arrival     // Arrival recommendations
}
```

**Departure Layover Flight Mapping:**
```javascript
// Requires: flight.departure_lo_time AND flight.departure_lo_flightnumber
{
  type: 'flight_departure_layover',
  title: "✈️ Layover: ATL → JFK",           // From departure_lo_from_airport → departure_lo_to_airport
  start: flight.departure_lo_time,           // "2025-10-10T12:00:00+00:00/2025-10-10T14:30:00+00:00"
  end: flight.departure_lo_time,             // Same as start (date range)
  description: "Confirmation: HWSV8Y\nFlight #: DL 456\nFrom: ATL\nTo: JFK",
  location: flight.departure_lo_from_airport, // "Atlanta International Airport"
  url: flight.flight_url,                    // "https://www.notion.so/26939e4a65a980f6839bd853232eaa52"
  airline: flight.departure_airline,         // "Delta"
  flightNumber: flight.departure_lo_flightnumber, // "DL 456"
  confirmation: flight.confirmation,          // "HWSV8Y"
  mainEvent: event.event_name                // Associated main event
}
```

**Return Layover Flight Mapping:**
```javascript
// Requires: flight.return_lo_time AND flight.return_lo_flightnumber
{
  type: 'flight_return_layover',
  title: "✈️ Layover: JFK → ATL",           // From return_lo_from_airport → return_lo_to_airport
  start: flight.return_lo_time,              // "2025-10-12T18:00:00+00:00/2025-10-12T20:30:00+00:00"
  end: flight.return_lo_time,                // Same as start (date range)
  description: "Confirmation: HWSV8Y\nFlight #: DL 789\nFrom: JFK\nTo: ATL",
  location: flight.return_lo_from_airport,  // "John F. Kennedy International Airport"
  url: flight.flight_url,                    // "https://www.notion.so/26939e4a65a980f6839bd853232eaa52"
  airline: flight.return_airline,           // "Delta"
  flightNumber: flight.return_lo_flightnumber, // "DL 789"
  confirmation: flight.confirmation,         // "HWSV8Y"
  mainEvent: event.event_name                // Associated main event
}
```

### 3. **Rehearsal Events**
**Triggers:** Top-level `Rehearsals` field

**Available Fields (6 total):**
- `rehearsal_time` - ISO 8601 date range (required)
- `rehearsal_pco` - Planning Center Online link
- `rehearsal_band` - Band personnel list
- `rehearsal_location` - Location name
- `rehearsal_address` - Full address
- `description` - Rehearsal description (e.g., "Rehearsal for 10/11/25 Midnight Hour - LA Wedding")

**Rehearsal Mapping:**
```javascript
// Requires: rehearsal.rehearsal_time (not null)
{
  type: 'rehearsal',
  title: "🎤 Rehearsal",                    // Fixed title
  start: rehearsal.rehearsal_time,          // "2025-09-11T17:00:00+00:00/2025-09-11T19:00:00+00:00"
  end: rehearsal.rehearsal_time,            // Same as start (date range)
  description: "Rehearsal for 10/11/25 Midnight Hour - LA Wedding\n\nBand Personnel:\nBass - Eric  🟢\nDrums - Diego  🟢\nGuitar - Silas  🟢\nKeys - Kevin  🟢\nVox 1 - Revel  🟢\nVox 2 - Dani  🟢\nVox 3 - Joe  🟢\nVox 4 - Ayo  🟢",
  location: rehearsal.rehearsal_address || rehearsal.rehearsal_location || "TBD",  // "123 W Bellevue Dr Ste 4 Pasadena, CA 91105⁠"
  url: rehearsal.rehearsal_pco             // "https://services.planningcenteronline.com/plans/81859026"
}
```

### 4. **Hotel Events**
**Triggers:** Top-level `Hotels` field

**Available Fields (9 total):**
- `title` - Hotel entry title
- `hotel_url` - Link to Notion page for this hotel
- `hotel_name` - Hotel name
- `hotel_phone` - Hotel phone number
- `hotel_address` - Full hotel address
- `confirmation` - Booking confirmation number
- `names_on_reservation` - Guest names
- `booked_under` - Primary booker name
- `dates_booked` - ISO 8601 date range (required)

**Hotel Mapping:**
```javascript
// Requires: hotel.dates_booked
{
  type: 'hotel',
  title: "🏨 " + (hotel.hotel_name || hotel.title || "Hotel"),  // "🏨 Hilton Garden Inn Sonoma County Airport"
  start: hotel.dates_booked,               // "2025-09-20T23:00:00+00:00/2025-09-21T18:00:00+00:00"
  end: hotel.dates_booked,                 // Same as start (date range)
  description: "Hotel Stay\nConfirmation: 3291242890\nPhone: (707) 545-0444\n\nNames on Reservation:\nJackie,Eric,Joakim,Dave,Payson,Byron,Diego,Gabe,Michael\nBooked Under: Diego\n\nNotion Link: https://www.notion.so/22a39e4a65a980418fc2dc12edd96217",
  location: hotel.hotel_address || hotel.hotel_name || "Hotel",  // "417 Aviation Blvd, Santa Rosa, CA 95403"
  url: hotel.hotel_url,                    // "https://www.notion.so/22a39e4a65a980418fc2dc12edd96217"
  confirmation: hotel.confirmation,        // "3291242890"
  hotelName: hotel.hotel_name,             // "Hilton Garden Inn Sonoma County Airport"
  hotelPhone: hotel.hotel_phone,           // "(707) 545-0444"
  namesOnReservation: hotel.names_on_reservation,  // "Jackie,Eric,Joakim,Dave,Payson,Byron,Diego,Gabe,Michael"
  bookedUnder: hotel.booked_under          // "Diego"
}
```

### 5. **Transportation Events**
**Triggers:** Top-level `Transportation` field

**Available Fields (7 total):**
- `title` - Transportation entry title (required)
- `transportation_url` - Link to Notion page for this transport
- `start` - Start time as ISO 8601 (required)
- `end` - End time as ISO 8601
- `location` - Pickup/dropoff location
- `description` - Driver info, passengers, notes
- `type` - Transport type (ground_transport_pickup, ground_transport_dropoff, ground_transport_meeting, ground_transport)

**Transportation Mapping:**
```javascript
// Requires: transport.start
{
  type: transport.type || 'ground_transport',  // "ground_transport_meeting"
  title: "🚙 " + (transport.title || "Ground Transport"),  // "🚙 MEET UP: Band Sprinter ( Wedding)"
  start: transport.start,                  // "2025-09-20T14:00:00+00:00"
  end: transport.end || transport.start,   // "2025-09-20T14:00:00+00:00"
  description: "Driver: Diego De la Rosa\nPassenger: Eric England,Diego De la Rosa,Gabriel Rudner,Michael Czaja,Joakim Toftgaard,Michael Campagna,Jacquelyn Foster\nMeet Up Info: Meetup Location: Sierra Madre Villa,149 N Halstead St, Pasadena, CA 91107,\nDriver Info:\nDiego - (626) 991-4302,\nMeetup Notes: Make sure you bring a coffee for Diego\n\nNotion Link: https://www.notion.so/22839e4a65a98008b326f8e0a9f17129",
  location: transport.location || "",      // "149 N Halstead St, Pasadena, CA 91107"
  url: transport.transportation_url        // "https://www.notion.so/22839e4a65a98008b326f8e0a9f17129"
}
```

### 6. **Team Calendar Events**
**Triggers:** Top-level `Team Calendar` field

**Available Fields (6 total):**
- `title` - Event title (required)
- `address` - Location address
- `date` - ISO 8601 date range (required)
- `notes` - Additional notes
- `dcos` - Downbeat Chain of Support context (formatted text)
- `notion_link` - Link to Notion page

**Team Calendar Mapping:**
```javascript
// Requires: teamEvent.date
{
  type: 'team_calendar',
  title: "📅 " + (teamEvent.title || "Team Event"),  // "📅 Office"
  start: teamEvent.date,                   // "2025-09-15T17:30:00+00:00/2025-09-16T01:30:00+00:00"
  end: teamEvent.date,                     // Same as start (date range)
  description: [teamEvent.dcos, teamEvent.notes].filter(Boolean).join("\n\n"),  // DCOS context followed by notes
  location: teamEvent.address || "",       // "123 W Bellevue Dr Ste 4, Pasadena CA 91105"
  url: teamEvent.notion_link || ""         // "https://www.notion.so/17839e4a65a980fb8409c4b2231408b9"
}
```

## Required JSON Structure

### "Calendar Data" Database Format
The database uses separate formula fields for each event type. Each field contains a JSON array:

```javascript
{
  "Events": "[{\"event_name\":\" Wedding\",\"notion_url\":\"https://www.notion.so/13839e4a65a9804c8d66d0574a4acbf6\",\"event_date\":\"2025-09-13T22:00:00+00:00/2025-09-14T06:00:00+00:00\",\"band\":\"Gold Standard\",\"event_personnel\":\"Audio: A1 - Adrian Alvarado 📷 DJ 🍸\\nAudio: A2 - Paul Kirz\\nBand: Bass - Eric England\\nBand: Drums - Diego De la Rosa\\nBand: Guitar - Pedro Cordeiro\\nBand: Keys - Andre Bernier\\nBand: Vox 1 - Byron Dodson\\nBand: Vox 2 - Danielle Mozeleski\\nBand: Vox 3 - Joseph Leone\\nBand: Vox 4 - Uchechi Ejelonu\\nHorns: Sax - Carlo Alfonso 🍸\\nHorns: Trombone - Kevin Hicks\\nHorns: Trumpet - Scott Bell\",\"calltime\":\"2025-09-13T22:00:00+00:00\",\"gear_checklist\":\"\",\"general_info\":\"Parking and Load In:...\",\"venue\":\"Casa Del Mar\",\"venue_address\":\"1910 Ocean Way, Santa Monica, CA 90405\",\"pay_total\":800,\"position\":\"Drums\",\"assignments\":\"Base + Rehearsal\"}]",
  
  "Flights": "[{\"confirmation\":\"HWSV8Y\",\"flight_url\":\"https://www.notion.so/26939e4a65a980f6839bd853232eaa52\",\"airport_arrival\":\"Domestic flights (within the U.S.) → Arrive 2 hours before departure. International flights → Arrive 3 hours before departure.\",\"flight_status\":\"Booked\",\"flight_type\":\"Round Trip\",\"departure_name\":\"Flight to JFK (Diego)\",\"departure_airline\":\"Delta\",\"departure_flightnumber\":\"DL 915\",\"departure_time\":\"2025-10-10T06:55:00+00:00/2025-10-10T15:30:00+00:00\",\"departure_airport\":\"1 World Way, Los Angeles, CA 90045\",\"departure_airport_name\":\"Los Angeles International Airport\",\"return_name\":\"Flight Return to LAX (Diego)\",\"return_airline\":\"Delta\",\"return_airport\":\"JFK Access Rd, Jamaica, NY 11430\",\"return_airport_name\":\"John F. Kennedy International Airport\",\"return_flightnumber\":\"DL 773\",\"return_time\":\"2025-10-12T16:55:00+00:00/2025-10-12T20:02:00+00:00\"}]",
  
  "Rehearsals": "[{\"rehearsal_time\":\"2025-09-11T17:00:00+00:00/2025-09-11T19:00:00+00:00\",\"rehearsal_pco\":\"https://services.planningcenteronline.com/plans/81859026\",\"rehearsal_band\":\"Bass - Eric  🟢\\nDrums - Diego  🟢\\nGuitar - Silas  🟢\\nKeys - Kevin  🟢\\nVox 1 - Revel  🟢\\nVox 2 - Dani  🟢\\nVox 3 - Joe  🟢\\nVox 4 - Ayo  🟢\",\"description\":\"Rehearsal for 9/14/25 Gold Standard - Santa Monica Wedding\",\"rehearsal_location\":\"Downbeat HQ\",\"rehearsal_address\":\"123 W Bellevue Dr Ste 4 Pasadena, CA 91105⁠\"}]",
  
  "Hotels": "[{\"title\":\"Hotel -  (Band)\",\"hotel_url\":\"https://www.notion.so/22a39e4a65a980418fc2dc12edd96217\",\"hotel_name\":\"Hilton Garden Inn Sonoma County Airport\",\"hotel_phone\":\"(707) 545-0444\",\"hotel_address\":\"417 Aviation Blvd, Santa Rosa, CA 95403\",\"confirmation\":\"3291242890\",\"names_on_reservation\":\"Jackie,Eric,Joakim,Dave,Payson,Byron,Diego,Gabe,Michael\",\"booked_under\":\"Diego\",\"dates_booked\":\"2025-09-20T23:00:00+00:00/2025-09-21T18:00:00+00:00\"}]",
  
  "Transportation": "[{\"title\":\"MEET UP: Band Sprinter ( Wedding)\",\"start\":\"2025-09-20T14:00:00+00:00\",\"end\":\"2025-09-20T14:00:00+00:00\",\"transportation_url\":\"https://www.notion.so/22839e4a65a98008b326f8e0a9f17129\",\"location\":\"149 N Halstead St, Pasadena, CA 91107\",\"description\":\"Driver: Diego De la Rosa\\nPassenger: Eric England,Diego De la Rosa,Gabriel Rudner,Michael Czaja,Joakim Toftgaard,Michael Campagna,Jacquelyn Foster\\nMeet Up Info: Meetup Location: Sierra Madre Villa,149 N Halstead St, Pasadena, CA 91107,\\nDriver Info:\\nDiego - (626) 991-4302,\\nMeetup Notes: Make sure you bring a coffee for Diego\",\"type\":\"ground_transport_meeting\"}]",
  
  "Team Calendar": "[{\"title\":\"Office\",\"address\":\"123 W Bellevue Dr Ste 4, Pasadena CA 91105\",\"date\":\"2025-09-15T17:30:00+00:00/2025-09-16T01:30:00+00:00\",\"notes\":\"\",\"dcos\":\"Reminder: Submit DCOS report by EOD\",\"notion_link\":\"https://www.notion.so/17839e4a65a980fb8409c4b2231408b9\"}]"
}
```

## API Response Structure

```javascript
{
  "personName": "Diego De la Rosa",
  "totalMainEvents": 31,          // Count of main events
  "totalCalendarEvents": 117,     // Count of all calendar events generated
  "breakdown": {
    "mainEvents": 31,             // Wedding/gig events
    "flights": 10,                // Flight departure + return + layover events  
    "rehearsals": 16,             // Rehearsal events
    "hotels": 7,                  // Hotel bookings
    "groundTransport": 10,        // Ground transportation events (pickup, dropoff, meeting)
    "teamCalendar": 53            // Office days and team events
  },
  "events": [
    // Array of all calendar events (main + flights + layovers + rehearsals + hotels + transport + team)
  ]
}
```

## Event Types Generated

1. **Main Events**: `type: 'main_event'` - One per event in Events array
2. **Flight Departures**: `type: 'flight_departure'` - One per flight with departure info
3. **Flight Returns**: `type: 'flight_return'` - One per flight with return info
4. **Flight Departure Layovers**: `type: 'flight_departure_layover'` - One per departure layover flight
5. **Flight Return Layovers**: `type: 'flight_return_layover'` - One per return layover flight
6. **Rehearsals**: `type: 'rehearsal'` - One per rehearsal with valid time
7. **Hotels**: `type: 'hotel'` - One per hotel booking
8. **Transportation**: `type: 'ground_transport_pickup'|'ground_transport_dropoff'|'ground_transport_meeting'|'ground_transport'` - One per transport event
9. **Team Calendar**: `type: 'team_calendar'` - One per office day or team event

## Calendar Integration

- **Endpoint**: `GET /calendar/:personId` - Uses "Calendar Data" database
- **ICS Format**: `GET /calendar/:personId?format=ics` - All events in calendar format
- **JSON Format**: `GET /calendar/:personId` - Structured data with event breakdown
- **Event Titles**: Include emojis (✈️ for flights, 🎤 for rehearsals, 🏨 for hotels, 🚙 for transport, 📅 for team calendar) for easy identification
- **Descriptions**: Include confirmation numbers, flight details, call times, Notion links, etc.
- **Links**: Events link back to Notion via URL fields (notion_url, flight_url, hotel_url, transportation_url, notion_link)
- **Date Ranges**: All times use ISO 8601 format with date ranges (e.g., "2025-09-13T22:00:00+00:00/2025-09-14T06:00:00+00:00")

## Optimization Notes

To ensure your Notion formula works within API timeout limits:
- **Limit events**: Process max 5-10 events at a time
- **Reduce complexity**: Avoid nested loops and complex string operations  
- **Test incrementally**: Start simple and add complexity gradually
- **Use date filters**: Limit to recent/upcoming events only

## Testing

Use the debug endpoint to verify data:

**Debug Endpoint:**
```bash
curl "https://calendar.downbeat.agency/debug/calendar-data/PERSON_ID"
```

**Calendar API:**
```bash
# JSON format
curl "https://calendar.downbeat.agency/calendar/PERSON_ID"

# ICS format (for calendar subscription)
curl "https://calendar.downbeat.agency/calendar/PERSON_ID?format=ics"
```
