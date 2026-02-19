# Handoff: Timezone Issues in Notion Calendar Feeds

## Implementation Status (February 19, 2026)

This handoff has now been implemented in server version `tz-fix-v8-main-event-compat`.

### What was implemented

1. **Smart calltime parsing (`parseCalltimeSmart`)**
   - If calltime includes `Z` or `±HH:MM`, it is parsed as UTC and converted to Pacific floating time.
   - If calltime has no offset, it is treated as Pacific face-value for backward compatibility.

2. **Main-event end compatibility correction (`maybeCorrectMainEventEnd`)**
   - Applies only to main-event `event_date` ranges where both start/end are offset-tagged.
   - Uses calltime (if present) as anchor start; otherwise uses parsed event start.
   - Builds fallback end from raw end clock anchored to anchor date, rolling +1 day when needed.
   - Applies correction only when:
     - parsed duration is implausible (`<=0h` or `>16h`), or
     - overnight guard is triggered (raw end clock earlier than anchor clock while parsed end stays same day).
   - Fallback is accepted only if corrected duration is `>0h` and `<=16h`.
   - Logs each correction with `[TZ-COMPAT] Main event end corrected (...)`.

3. **Shared main-event time pipeline (`resolveMainEventTimes`)**
   - Used in all three main-event paths:
     - regeneration flow
     - admin calendar processing
     - direct `/calendar/:personId` flow
   - Sequence:
     - parse `event_date`
     - parse calltime smart
     - apply main-event end compatibility correction
     - apply calltime override against corrected end date

4. **Calltime display update**
   - `formatCallTime()` now uses smart calltime parsing so description calltimes match corrected event start behavior.

5. **Debug visibility**
   - `/debug/parse-test` now includes:
     - `calltime_smart_utc`
     - `calltime_smart_hours`
     - `end_compat_applied`
     - `end_compat_reason`

### Compatibility mode removal criteria

Keep compatibility mode enabled until production data proves stable. Remove `maybeCorrectMainEventEnd` when all are true:

1. Notion Events formula consistently outputs true UTC moments for both range start and range end.
2. `end_compat_applied` remains false (or near zero) across multiple regeneration cycles.
3. Spot checks of known problem events (for example, West Hollywood Corporate) match expected Pacific wall-clock times without compatibility corrections.

## Project Overview

This is a Node.js/Express app deployed on Railway that generates ICS calendar feeds from a Notion database. It queries a "Calendar Data" Notion database (via `CALENDAR_DATA_DATABASE_ID`), parses event JSON from formula properties, and generates ICS files cached in Redis.

- **Repo**: `/Users/diego/Desktop/notion-calendar-feeds`
- **Main file**: `index.js` (~7200 lines)
- **Deployed on**: Railway (auto-deploys from `origin/main`)
- **Server version**: `tz-fix-v8-main-event-compat`
- **Key endpoint**: `/calendar/:personId` (returns ICS or JSON)
- **Regeneration**: `/regenerate/:personId` triggers background cache rebuild

---

## The Core Problem (Historical, Pre-v8)

**Every single event's times are wrong in the ICS output.** Times are shifted by 7-8 hours (the UTC-Pacific offset). Out of 48 events tested, 0 are correct.

Example — "West Hollywood Corporate":
- **Expected**: Jan 23, 11:00 AM – 11:00 PM (Pacific)
- **ICS shows**: Jan 23, 7:00 PM – Jan 24, 3:00 PM

---

## Root Cause: Notion Formula Changed, But Code Wasn't Updated

The user recently **changed `event_date` from a Notion date property to a formula property**. This changed the format of dates inside the Events formula JSON string.

### What the Notion API actually returns (verified via `/debug/parse-test`)

```json
{
  "raw_event_date": "2026-01-23T19:00:00+00:00/2026-01-24T23:00:00+00:00",
  "raw_calltime": "2026-01-23T19:00:00+00:00"
}
```

### What the user expected (from their own JSON export with correct values)

```json
{
  "event_date": "2026-01-23T11:00:00-08:00/2026-01-23T23:00:00-08:00",
  "calltime": "2026-01-23T11:00:00-08:00"
}
```

### The mismatch

| Field | User's JSON (correct values) | Notion API output (what server gets) |
|-------|------------------------------|--------------------------------------|
| **Start** | `2026-01-23T11:00:00-08:00` (11 AM PST) | `2026-01-23T19:00:00+00:00` (same moment, 7 PM UTC) ✅ |
| **End** | `2026-01-23T23:00:00-08:00` (11 PM PST = 7 AM UTC next day) | `2026-01-24T23:00:00+00:00` (11 PM UTC Jan 24) ❌ |
| **Calltime** | `2026-01-23T11:00:00-08:00` (11 AM PST) | `2026-01-23T19:00:00+00:00` (7 PM UTC — face value is UTC, not Pacific) ❌ |

**The start time converts correctly** (both representations are the same UTC moment). But the **end time is 16 hours wrong** — the Notion formula appears to be taking the Pacific hour value (23 = 11 PM) and stamping it as UTC (`+00:00`), when the correct UTC would be `07:00+00:00` the next day.

The **calltime** has a similar issue: `19:00+00:00` means 7 PM UTC = 11 AM PST, which is the correct *moment*, but `extractLocalComponents()` reads the face-value hours as `19` (not `11`), so the calltime override produces 7 PM instead of 11 AM.

---

> Historical note: The remaining sections below are preserved from pre-v8 debugging context.
> For current behavior, use the "Implementation Status" section at the top of this file.

## How the Code Worked Pre-v8 (Historical)

### Data Flow
```
Notion "Calendar Data" DB
  → Events formula property (JSON string with all events)
  → getCalendarDataFromDatabase() parses JSON (index.js:354-375)
  → event.event_date = "2026-01-23T19:00:00+00:00/2026-01-24T23:00:00+00:00"
  → parseUnifiedDateTime(event.event_date) (index.js:561-780)
    → new Date("2026-01-23T19:00:00+00:00") = 2026-01-23T19:00:00Z
    → convertUTCToPacific(): subtract 8h → 2026-01-23T11:00:00Z  ← stored in UTC slot as Pacific floating
    → Date.UTC reconstruction → final Date object with Pacific in UTC slots
  → calltime override (index.js:950-962)
    → extractLocalComponents(event.calltime) → reads face-value hours from ISO string
    → Anchors calltime hours to the end date
  → ical-generator with floating:true → writes UTC slots directly to ICS (no Z suffix)
```

### Key Functions (all in index.js)

| Function | Lines | Purpose |
|----------|-------|---------|
| `convertUTCToPacific()` | 278-280 | Subtracts 7/8h from UTC Date to get Pacific |
| `extractLocalComponents()` | 545-556 | Regex-extracts Y/M/D/H/M/S from ISO string face value, ignoring offset |
| `parseUnifiedDateTime()` | 561-780 | Main parser. Handles `@` format, `/` range format, single ISO. Default: treats as UTC → converts to Pacific. `faceValue: true`: uses face-value hours directly |
| `getCalendarDataFromDatabase()` | 354-375 | Queries Notion, returns parsed JSON events |
| Event processing (regen) | 945-1017 | Processes events into ICS during regeneration |
| Event processing (direct) | 6380-6423 | Same logic for direct `/calendar/:personId` requests |
| Calltime override | 950-962 | `extractLocalComponents(calltime)` → anchor hours to end date |

### parseUnifiedDateTime — the `/` range path (line 681+)

```javascript
// For "2026-01-23T19:00:00+00:00/2026-01-24T23:00:00+00:00":
let actualStartDate = new Date(firstStr);   // → 2026-01-23T19:00:00Z
let actualEndDate = new Date(secondStr);     // → 2026-01-24T23:00:00Z

// Default path (not faceValue):
const pacific = convertUTCToPacific(actualStartDate);
// 19:00Z - 8h = 11:00Z → stored as Pacific floating
actualStartDate = new Date(Date.UTC(pacific.getUTCFullYear(), ...));

// Same for end:
const pacificEnd = convertUTCToPacific(actualEndDate);
// 23:00Z - 8h = 15:00Z → 3 PM Pacific (WRONG — should be 11 PM)
actualEndDate = new Date(Date.UTC(pacificEnd.getUTCFullYear(), ...));
```

### Calltime Override (line 950-962)

```javascript
if (event.calltime && eventTimes?.end) {
  const ctComponents = extractLocalComponents(event.calltime);
  // For "2026-01-23T19:00:00+00:00" → hours=19 (face value of UTC string)
  // This SHOULD be 11 (Pacific), but the Notion formula outputs UTC face value
  const endDate = eventTimes.end;
  const ctStart = new Date(Date.UTC(
    endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate(),
    ctComponents.hours, ctComponents.minutes, ctComponents.seconds
  ));
  if (ctStart.getTime() > endDate.getTime()) {
    ctStart.setTime(ctStart.getTime() - 24 * 60 * 60 * 1000);
  }
  eventTimes.start = ctStart;
}
```

---

## Two Separate Issues to Fix (Now Addressed in v8)

### Issue 1: Notion formula outputs wrong end times

The Notion "Events" formula that builds the JSON string is producing **incorrect end times**. The formula takes the Pacific hour value and stamps it as UTC. For an event ending at 11 PM Pacific:
- Correct UTC: `2026-01-24T07:00:00+00:00` (11 PM PST = next day 7 AM UTC)
- Formula outputs: `2026-01-24T23:00:00+00:00` (keeps "23" hour but marks as UTC)

**This needs to be fixed in the Notion formula itself.** The code's `convertUTCToPacific()` correctly converts valid UTC → Pacific. The input is just wrong.

### Issue 2: Calltime uses face-value hours from UTC string

`extractLocalComponents("2026-01-23T19:00:00+00:00")` returns `hours: 19`. This was designed for Pacific face-value strings (e.g., `"2026-01-23T11:00:00"` with no offset). Now that Notion returns UTC strings with `+00:00`, the face-value extraction grabs UTC hours instead of Pacific hours.

**Fix options:**
- A) Fix the Notion formula to output calltime as Pacific face-value (no offset or with `-08:00`)
- B) Change calltime override code to parse the calltime through `convertUTCToPacific` first, then extract components
- C) Use `parseUnifiedDateTime(event.calltime)` (default, not faceValue) and extract hours from the result

---

## Verification Data

### Full comparison: Source JSON vs ICS output (48 events, 0 correct)

All events are wrong. Pattern:
- **Start times**: shifted by +8h (UTC value used as Pacific), then partially corrected by calltime override (but calltime reads UTC face-value hours)
- **End times**: shifted by -8h (Notion formula outputs Pacific hour as UTC, then code subtracts another 8h)

Example events:
```
West Hollywood Corporate:  want 1/23 11AM-11PM,  got 1/23 7PM – 1/24 3PM
Los Angeles Wedding:       want 2/15 11:30AM-10PM, got 2/16 11:30AM – 2/16 2PM
Palm Springs Wedding:      want 2/7 10AM – 2/8 1AM, got 2/8 10AM – 2/8 5PM
```

### Debug endpoint verification

`GET /debug/parse-test?person=<id>&event=West+Hollywood` returns:
```json
{
  "raw_event_date": "2026-01-23T19:00:00+00:00/2026-01-24T23:00:00+00:00",
  "parsed_event_start_hours": 11,
  "parsed_event_end_hours": 15,
  "calltime_faceValue_hours": 19,
  "calltime_utcConvert_hours": 11
}
```

- `parsed_event_start_hours: 11` ← correct (19:00 UTC → 11 AM Pacific)
- `parsed_event_end_hours: 15` ← WRONG (should be 23 = 11 PM Pacific; 23:00 UTC → 3 PM Pacific)
- `calltime_faceValue_hours: 19` ← WRONG (reads UTC face value, should be 11 for Pacific)
- `calltime_utcConvert_hours: 11` ← correct (proper UTC→Pacific conversion)

---

## Environment

- **Railway env vars needed**: `NOTION_API_KEY`, `CALENDAR_DATA_DATABASE_ID`, `REDIS_URL`
- **Server timezone**: UTC (confirmed via debug endpoint `serverTZ: "UTC"`)
- **ICS generation**: `ical-generator` with `floating: true` — outputs Date's UTC slot values without `Z` suffix
- **Person ID for testing**: `51b050cc476541aabe462a8e3b0632ba`

---

## Implemented Fix Path

1. **Notion formula contract fix (external to repo)**
   - Ensure Events formula outputs true UTC for `event_date` start/end and `calltime`.
   - Avoid rebuilding hour/minute then stamping `+00:00`.

2. **Server compatibility mode for malformed end-times**
   - Implemented via `maybeCorrectMainEventEnd` with a moderate heuristic (`<=16h` expected duration + overnight guard).
   - Limited to main-event `event_date` only.

3. **Smart calltime override**
   - Implemented via `parseCalltimeSmart` + `applyCalltimeOverride`.
   - Handles both UTC-tagged and legacy Pacific no-offset calltime values.

---

## Files Reference

| File | Purpose |
|------|---------|
| `index.js` | Main application — all server logic, parsing, ICS generation |
| `DATES_AND_TIMEZONES.md` | Documentation of the App Data timezone model (separate but related system) |
| `regen.js` | Helper script to trigger regeneration from CLI |
| `validate-events.json` | Sample event data (3 events, from earlier debugging) |
