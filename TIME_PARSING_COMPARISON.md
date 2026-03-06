# Time Parsing Baseline and Contract

## Baseline Inputs Collected

- **Raw Notion sample (local snapshot):**
  - `validate-events.json`
  - `temp-validate.json`
  - These samples currently include explicit offsets like `-08:00`.
- **Formula reference file:**
  - `notion-formula-updated.txt`
  - Shows the Notion formulas that generate date strings.
- **Railway live output attempt:**
  - `GET /calendar/:personId?format=json` returned:
    - `{"error":"Calendar cache is empty","message":"Your calendar is being regenerated..."}`
  - `GET /regenerate/:personId/status` reported `failed` during this historical run before the synchronous regen flow replaced status polling.
  - `GET /debug/parse-test` timed out during this run.

## Unified Parsing Contract (Implemented)

1. **No timezone suffix (new formula output):**
   - Parse as **Pacific wall time** (floating), not UTC.
2. **Explicit timezone suffix (`Z`, `+00:00`, `-08:00`, etc.):**
   - Parse as absolute time, then normalize to floating Pacific.
3. **Start-date ownership rule:**
   - Event date always comes from parsed `start`.
   - Never build a start date from the end date.
4. **Overnight rule:**
   - If parsed `end < start`, roll `end` forward by one day.
   - Do not swap start and end.

## Mapping and Conversion Alignment

- **Main events**
  - `resolveMainEventTimes()` -> `parseUnifiedDateTime()` -> `applyCalltimeOverride()`
  - Calltime now anchors to **start date**, not end date.
- **Flights**
  - `getFlightLegTimes()` now returns `endParsed.end` (not `endParsed.start`).
  - Travel admin processing now also uses parsed `.end` for arrival.
- **Hotels**
  - Fallback conversion logic now uses unified parser for `check_in` and `check_out` separately.
  - Removed duplicate manual DST math in event-build paths.
- **Transportation**
  - Uses unified parser for `start` and optional `end`.
  - If no parsable end, defaults to 30 minutes after start.
  - Keeps `Date` objects (no ISO-string re-conversion before event push).
- **Rehearsals / Team / Office**
  - Continue to use unified parser; no special timezone branch needed.

## Why This Fix Addresses Reported Bugs

- Fixes cases where overnight events showed end-time/day as the start.
- Keeps event date tied to the start timestamp for all types.
- Handles both legacy offset-tagged values and new no-offset values under one parser path.

## Regression Checks Run

Ran local `debug/parse-test` against the updated parser:

- Input: `2026-03-14T14:00:00/2026-03-14T23:30:00`
  - Output: `startISO=2026-03-14T14:00:00.000Z`, `endISO=2026-03-14T23:30:00.000Z`
  - Confirms no-offset timestamps are preserved as floating wall-time values.
- Input: `2026-02-21T22:00:00+00:00/2026-02-22T02:00:00+00:00`
  - Output: `startISO=2026-02-21T14:00:00.000Z`, `endISO=2026-02-21T18:00:00.000Z`
  - Confirms explicit-offset timestamps are normalized to Pacific floating time.
- Input: `2026-04-11T17:00:00/2026-04-11T01:00:00`
  - Output: `startISO=2026-04-11T17:00:00.000Z`, `endISO=2026-04-12T01:00:00.000Z`
  - Confirms overnight rollover moves end to next day (without moving start day).
