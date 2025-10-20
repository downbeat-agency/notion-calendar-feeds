# Fix Flight Countdown Timezone Display

## Problem

The flight countdown page has multiple issues:

1. **Incorrect green times** (3:15 AM / 8:48 AM) - Displayed in wrong timezone
2. **Hardcoded white text** - Terminal, Gate, Baggage, Airport names, and the "correct" times are all hardcoded placeholders for the HNL→LAX flight
3. **Hardcoded flight duration** - Calculates arrival as departure + 5h33m instead of parsing the actual arrival time from the date range

## What Data We Have from Notion

From `EVENT_MAPPING.md`, the countdown page receives these URL parameters:
- `departure` - **ISO 8601 date range**: `"2025-10-19T13:15:00-10:00/2025-10-19T21:47:00-07:00"`
  - This includes BOTH departure time AND arrival time with timezones!
  - Format: `startTime/endTime`
- `flight` - Flight number (e.g., "DL 915")
- `airline` - Airline name (e.g., "Delta")
- `route` - Route string (e.g., "HNL-LAX")
- `confirmation` - Booking confirmation
- `departure_airport_name` - Full airport name (e.g., "Los Angeles International Airport")
- `return_airport_name` - Full airport name (e.g., "John F. Kennedy International Airport")

## What We DON'T Have (until flight tracking API)
- ❌ Terminal numbers
- ❌ Gate numbers  
- ❌ Baggage belt
- ❌ Real-time status (delays, "1m early", etc.)

## Solution

### 1. Parse the ISO Date Range Properly
The `departure` parameter contains a **range** with both times:
```javascript
// Current (wrong): Only parses first part
const departureTime = new Date(departureTimeStr);

// Fixed: Split the range and parse both
const [departureStr, arrivalStr] = departureTimeStr.split('/');
const departureTime = new Date(departureStr);
const arrivalTime = new Date(arrivalStr);
```

### 2. Use Floating Time Approach (Like Main Calendar)
Instead of timezone-aware display, use the same "floating time" approach as the main calendar system:

```javascript
// Convert to floating time (like index.js lines 295-298)
const isDST = isDSTDate(departureTime);
const offsetHours = isDST ? 7 : 8; // PDT is UTC-7, PST is UTC-8
departureTime.setHours(departureTime.getHours() + offsetHours);
arrivalTime.setHours(arrivalTime.getHours() + offsetHours);

// Display as floating time (no timezone conversion)
const departureFormatted = departureTime.toLocaleTimeString('en-US', {
  hour: 'numeric',
  minute: '2-digit',
  hour12: true
});
```

### 3. Use Real Airport Names from Notion
Add URL parameters for airport names and use them:
```javascript
// In index.js, add to generateFlightCountdownUrl():
departureAirportName: flight.departure_airport_name || 'Airport',
returnAirportName: flight.return_airport_name || 'Airport'

// In countdown page:
const departureAirportName = getUrlParameter('departureAirportName') || 'Airport';
const arrivalAirportName = getUrlParameter('arrivalAirportName') || 'Airport';
```

### 4. Remove Hardcoded/Unavailable Data
Delete from HTML:
- Hardcoded airport names ("Daniel K Inouye Intl", etc.) - lines 256, 276
- Terminal/Gate/Baggage info (lines 257, 277)
- Hardcoded white times (lines 261, 281)
- "1m early" status text

Keep only:
- Airport codes (HNL, LAX) - extracted from route parameter
- Calculated status ("Departed", "En route", "Arrived") based on current time

### 5. Calculate Duration and Update Status Logic
Calculate actual flight duration and use real arrival time:
```javascript
// Duration calculation - no floating time conversion needed
const durationMs = arrivalTime - departureTime;
const durationHours = Math.floor(durationMs / (1000 * 60 * 60));
const durationMinutes = Math.floor((durationMs % (1000 * 60 * 60)) / (1000 * 60));

// Status logic - use actual arrival time
const timeToArrival = arrivalTime - now;
```

## Implementation Steps

1. **Update `index.js`** - Add airport name parameters to `generateFlightCountdownUrl()`
2. **Update `flight-countdown-dynamic.html`**:
   - Parse date range to get both departure AND arrival times
   - Apply floating time conversion (same as main calendar)
   - Remove all hardcoded HTML content
   - Display real airport names from URL parameters
   - Use actual arrival time for countdown/status

## Files to Modify
- `index.js` - Add airport name parameters to countdown URL
- `public/flight-countdown-dynamic.html` - Parse both times, remove hardcoded data

## Benefits
- Times display correctly regardless of user's timezone (floating time)
- Uses real data from Notion instead of hardcoded placeholders
- Uses actual arrival time (not calculated with hardcoded duration)
- Airport names display correctly from Notion
- Cleaner UI without unavailable terminal/gate/baggage info
