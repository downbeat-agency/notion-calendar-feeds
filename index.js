import 'dotenv/config';
import express from 'express';
import { Client } from '@notionhq/client';
import ical from 'ical-generator';
import { createClient } from 'redis';
import path from 'path';
import axios from 'axios';

// Server refresh - October 1, 2025
// Updated with event_personnel field support - October 8, 2025
// Retry deployment after network issues resolved
// Force deployment - testing event_personnel integration

const app = express();
const port = process.env.PORT || 3000;
const notion = new Client({ 
  auth: process.env.NOTION_API_KEY,
  timeoutMs: 90000 // 90 seconds - longer than Railway's 60s timeout to handle slow Notion responses
});

// Redis client setup
let redis = null;
let cacheEnabled = false;

try {
  if (process.env.REDIS_URL) {
    redis = createClient({
      url: process.env.REDIS_URL
    });

    redis.on('error', (err) => {
      console.error('Redis Client Error:', err.message);
      cacheEnabled = false;
    });
    
    redis.on('connect', () => {
      console.log('✅ Redis connected successfully');
      cacheEnabled = true;
    });

    // Connect to Redis
    await redis.connect();
  } else {
    console.warn('⚠️  REDIS_URL not configured - caching disabled');
  }
} catch (err) {
  console.error('Failed to connect to Redis:', err.message);
  console.warn('⚠️  Continuing without cache');
  redis = null;
  cacheEnabled = false;
}

// FlightAware AeroAPI configuration
const FLIGHTAWARE_API_KEY = process.env.FLIGHTAWARE_API_KEY;
const FLIGHTAWARE_BASE_URL = 'https://aeroapi.flightaware.com/aeroapi';

// FlightAware API helper functions
async function fetchFlightStatus(airline, flightNumber, departureDate) {
  if (!FLIGHTAWARE_API_KEY) {
    throw new Error('FlightAware API key not configured');
  }

  // Create flight ident (e.g., "DL915")
  const ident = `${airline}${flightNumber}`.replace(/\s+/g, '');
  
  // Create date range for the flight (24 hours before and after departure)
  const depDate = new Date(departureDate);
  const startDate = new Date(depDate.getTime() - 24 * 60 * 60 * 1000);
  const endDate = new Date(depDate.getTime() + 24 * 60 * 60 * 1000);
  
  const start = startDate.toISOString().split('T')[0];
  const end = endDate.toISOString().split('T')[0];

  try {
    const response = await axios.get(`${FLIGHTAWARE_BASE_URL}/flights/${ident}`, {
      headers: {
        'x-apikey': FLIGHTAWARE_API_KEY
      },
      params: {
        start: start,
        end: end,
        max_pages: 1
      },
      timeout: 10000 // 10 second timeout
    });

    if (response.data && response.data.flights && response.data.flights.length > 0) {
      // Find the flight closest to our departure date
      const flights = response.data.flights;
      const targetFlight = flights.find(flight => {
        const flightDate = new Date(flight.scheduled_out);
        return Math.abs(flightDate - depDate) < 24 * 60 * 60 * 1000; // Within 24 hours
      }) || flights[0]; // Fallback to first flight

      return {
        ident: targetFlight.ident,
        status: targetFlight.status,
        scheduled_out: targetFlight.scheduled_out,
        estimated_out: targetFlight.estimated_out,
        actual_out: targetFlight.actual_out,
        scheduled_in: targetFlight.scheduled_in,
        estimated_in: targetFlight.estimated_in,
        actual_in: targetFlight.actual_in,
        origin: targetFlight.origin,
        destination: targetFlight.destination,
        origin_gate: targetFlight.origin_gate,
        destination_gate: targetFlight.destination_gate,
        origin_terminal: targetFlight.origin_terminal,
        destination_terminal: targetFlight.destination_terminal,
        baggage_claim: targetFlight.baggage_claim,
        delay: targetFlight.delay
      };
    }
    
    return null;
  } catch (error) {
    console.error('FlightAware API error:', error.response?.status, error.response?.data || error.message);
    throw error;
  }
}

// Serve static files from public directory
app.use(express.static('public'));

// Use environment variable for Personnel database ID
const PERSONNEL_DB = process.env.PERSONNEL_DATABASE_ID;
const CALENDAR_DATA_DB = process.env.CALENDAR_DATA_DATABASE_ID;
const ADMIN_CALENDAR_PAGE_ID = process.env.ADMIN_CALENDAR_PAGE_ID;
const TRAVEL_CALENDAR_PAGE_ID = process.env.TRAVEL_CALENDAR_PAGE_ID;
const BLOCKOUT_CALENDAR_PAGE_ID = process.env.BLOCKOUT_CALENDAR_PAGE_ID;

// Cache TTL in seconds (30 minutes by default)
const CACHE_TTL = parseInt(process.env.CACHE_TTL) || 1800;
const CALENDAR_DATA_INDEX_CACHE_KEY = 'calendar:index:calendar_data_rows:v1';
const CALENDAR_DATA_INDEX_STATE_CACHE_KEY = 'calendar:index:calendar_data_rows:build_state:v1';
const CALENDAR_DATA_INDEX_TTL = Number(process.env.CALENDAR_DATA_INDEX_TTL || 24 * 60 * 60);
const CALENDAR_DATA_INDEX_BUILD_PAGES_PER_CYCLE = Number(process.env.CALENDAR_DATA_INDEX_BUILD_PAGES_PER_CYCLE || 10);
const LOG_DEDUP_WINDOW_MS = Number(process.env.LOG_DEDUP_WINDOW_MS || 30000);
const LOG_VERBOSE = String(process.env.LOG_VERBOSE || 'false').toLowerCase() === 'true';
const logDedupState = new Map();

function verboseLog(...args) {
  if (LOG_VERBOSE) {
    console.log(...args);
  }
}

async function setCalendarCache(key, value) {
  if (!redis || !cacheEnabled) {
    return;
  }
  await redis.setEx(key, CACHE_TTL, value);
}

async function getCachedJson(key) {
  if (!redis || !cacheEnabled) {
    return null;
  }

  const raw = await redis.get(key);
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    console.warn(`⚠️  Failed to parse cached JSON for ${key}: ${error.message}`);
    return null;
  }
}

async function setCachedJson(key, value, ttlSeconds) {
  if (!redis || !cacheEnabled) {
    return;
  }

  await redis.setEx(key, ttlSeconds, JSON.stringify(value));
}

function normalizeCalendarDataIndexEntries(entries) {
  if (!Array.isArray(entries)) {
    return [];
  }

  const deduped = new Map();
  for (const entry of entries) {
    if (!entry || typeof entry.pageId !== 'string' || typeof entry.personId !== 'string') {
      continue;
    }
    deduped.set(entry.pageId, {
      pageId: entry.pageId,
      personId: entry.personId
    });
  }

  return Array.from(deduped.values());
}

function logWithDedup(key, message, level = 'log') {
  const now = Date.now();
  const prev = logDedupState.get(key);
  if (!prev || now - prev.lastLoggedAt >= LOG_DEDUP_WINDOW_MS) {
    const logger = level === 'log' ? verboseLog : console[level].bind(console);
    if (prev?.suppressed > 0) {
      logger(`${message} (suppressed ${prev.suppressed} similar logs in dedup window)`);
    } else {
      logger(message);
    }
    logDedupState.set(key, { lastLoggedAt: now, suppressed: 0 });
    return;
  }
  prev.suppressed += 1;
  logDedupState.set(key, prev);
}

// Helper function to generate flight countdown URL
function generateFlightCountdownUrl(flightData, direction = 'departure') {
  const baseUrl = (process.env.BASE_URL || 'https://calendar.downbeat.agency').replace(/\/$/, '');
  
  // Always use URL params since we already have all the flight data
  const params = new URLSearchParams({
    flight: flightData.flightNumber || 'N/A',
    departure: flightData.departureTime,
    airline: flightData.airline || 'N/A',
    route: flightData.route || 'N/A',
    confirmation: flightData.confirmation || 'N/A',
    departureCode: flightData.departureCode || 'N/A',
    arrivalCode: flightData.arrivalCode || 'N/A',
    departureName: flightData.departureName || 'N/A',
    arrivalName: flightData.arrivalName || 'N/A'
  });
  return `${baseUrl}/flight-countdown-modern.html?${params.toString()}`;
}

// Helper function to get appropriate alarms for each event type
function getAlarmsForEvent(eventType, eventTitle = '') {
  // Skip alarms for OOO events
  if (eventTitle && eventTitle.includes('⛔️') && eventTitle.toUpperCase().includes('OOO')) {
    return [];
  }
  
  const alarmConfigs = {
    // FLIGHTS: 3 hours before
    'flight_departure': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_return': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_departure_layover': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    'flight_return_layover': [
      { type: 'display', trigger: 10800 }   // 3 hours
    ],
    
    // MAIN EVENTS: 1 hour before
    'main_event': [
      { type: 'display', trigger: 3600 }    // 1 hour
    ],
    
    // REHEARSALS: 24 hours before
    'rehearsal': [
      { type: 'display', trigger: 86400 }   // 24 hours
    ],
    
    // HOTELS: 4 hours before check-in
    'hotel': [
      { type: 'display', trigger: 14400 }   // 4 hours
    ],
    
    // TRANSPORTATION: 45 mins for pickup/meetup ONLY
    'ground_transport_pickup': [
      { type: 'display', trigger: 2700 }    // 45 minutes
    ],
    'ground_transport_meeting': [
      { type: 'display', trigger: 2700 }    // 45 minutes
    ],
    'ground_transport_dropoff': [],         // NO ALARM
    'ground_transport': [],                 // NO ALARM
    
    // TEAM CALENDAR: None
    'team_calendar': [],                    // NO ALARM
    'event_note_reminder': []              // NO ALARM
  };
  
  return alarmConfigs[eventType] || [];
}

// Helper function to convert timezone-aware ISO 8601 to Pacific time (updated v2)
function convertToPacific(isoString) {
  if (!isoString) return null;
  
  try {
    // Parse the ISO string with timezone offset (e.g., -07:00, -08:00)
    const date = new Date(isoString);
    
    if (isNaN(date.getTime())) {
      console.warn('Invalid date string:', isoString);
      return null;
    }
    
    // The date is already correctly parsed with timezone information
    // Just return it as-is since it's already in the correct timezone
    return date;
  } catch (e) {
    console.warn('Failed to parse ISO date:', isoString, e);
    return null;
  }
}

// Helper function to determine Pacific timezone offset
function getPacificOffset(date) {
  // Simple check: March 9 to November 2, 2025 should be PDT (UTC-7)
  // This is a simplified check - in production you'd want a proper timezone library
  const month = date.getMonth() + 1; // getMonth() is 0-based
  const day = date.getDate();
  
  if (month > 3 && month < 11) return '-07:00'; // PDT
  if (month === 3 && day >= 9) return '-07:00'; // PDT
  if (month === 11 && day <= 2) return '-07:00'; // PDT
  return '-08:00'; // PST
}

// Helper function to check if a date is in DST period
function isDSTDate(date) {
  const year = date.getFullYear();
  const month = date.getMonth();
  const day = date.getDate();
  
  // DST starts second Sunday in March at 2 AM
  const marchFirst = new Date(year, 2, 1); // March 1
  const dstStart = new Date(year, 2, 1 + (7 - marchFirst.getDay() + 7) % 7 + 7); // Second Sunday
  
  // DST ends first Sunday in November at 2 AM
  const novFirst = new Date(year, 10, 1); // November 1
  const dstEnd = new Date(year, 10, 1 + (7 - novFirst.getDay()) % 7); // First Sunday
  
  const checkDate = new Date(year, month, day);
  return checkDate >= dstStart && checkDate < dstEnd;
}

// Precise DST check for a specific UTC moment.
// DST starts: second Sunday of March at 10:00 UTC (2 AM PST)
// DST ends:   first Sunday of November at 9:00 UTC (2 AM PDT)
function isPacificDSTAtUTC(utcDate) {
  const year = utcDate.getUTCFullYear();

  const marchFirst = new Date(Date.UTC(year, 2, 1));
  const secondSunday = 1 + ((7 - marchFirst.getUTCDay()) % 7) + 7;
  const dstStartUTC = new Date(Date.UTC(year, 2, secondSunday, 10, 0, 0));

  const novFirst = new Date(Date.UTC(year, 10, 1));
  const firstSunday = 1 + ((7 - novFirst.getUTCDay()) % 7);
  const dstEndUTC = new Date(Date.UTC(year, 10, firstSunday, 9, 0, 0));

  return utcDate >= dstStartUTC && utcDate < dstEndUTC;
}

function convertUTCToPacific(utcDate) {
  const offsetHours = isPacificDSTAtUTC(utcDate) ? 7 : 8;
  return new Date(utcDate.getTime() - offsetHours * 60 * 60 * 1000);
}

// Helper function to format ISO timestamp to readable time (e.g., "1:30 PM")
// Uses smart calltime parsing to support both UTC-tagged and Pacific face-value calltime strings.
function formatCallTime(isoTimestamp) {
  if (!isoTimestamp || typeof isoTimestamp !== 'string') {
    return isoTimestamp;
  }

  const parsed = parseCalltimeSmart(isoTimestamp);
  const date = parsed?.start instanceof Date ? parsed.start : null;

  if (!date || isNaN(date.getTime())) {
    // Fallback: parse as-is with no timezone conversion.
    const fallback = new Date(isoTimestamp);
    if (!isNaN(fallback.getTime())) {
      return formatTimeParts(fallback.getUTCHours(), String(fallback.getUTCMinutes()).padStart(2, '0'));
    }
    return isoTimestamp;
  }

  const hours = date.getUTCHours();
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  return formatTimeParts(hours, minutes);
}

function formatTimeParts(hours24, minutes) {
  let hours = hours24;
  const period = hours >= 12 ? 'PM' : 'AM';
  if (hours === 0) {
    hours = 12;
  } else if (hours > 12) {
    hours -= 12;
  }
  if (minutes === '00') {
    return `${hours} ${period}`;
  }
  return `${hours}:${minutes} ${period}`;
}

function formatFloatingTimeFromDate(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) return '';
  return formatTimeParts(
    date.getUTCHours(),
    String(date.getUTCMinutes()).padStart(2, '0')
  );
}

const NOTION_RATE_LIMIT_RPS = Number(process.env.NOTION_RATE_LIMIT_RPS || 3);
const NOTION_MIN_INTERVAL_MS = Math.max(1, Math.floor(1000 / NOTION_RATE_LIMIT_RPS));
const NOTION_MAX_RETRY_BUDGET_MS = Number(process.env.NOTION_MAX_RETRY_BUDGET_MS || 120000);
const NOTION_CIRCUIT_WINDOW_MS = Number(process.env.NOTION_CIRCUIT_WINDOW_MS || 60000);
const NOTION_CIRCUIT_FAILURE_THRESHOLD = Number(process.env.NOTION_CIRCUIT_FAILURE_THRESHOLD || 10);
const NOTION_CIRCUIT_COOLDOWN_MS = Number(process.env.NOTION_CIRCUIT_COOLDOWN_MS || 90000);
const ENABLE_CALENDAR_DB_FALLBACK = String(process.env.ENABLE_CALENDAR_DB_FALLBACK || 'false').toLowerCase() === 'true';
const DEFAULT_REGEN_CONCURRENCY = Number(process.env.REGEN_WORKER_CONCURRENCY || 6);
const BACKGROUND_REGEN_CONCURRENCY = Number(process.env.BACKGROUND_REGEN_CONCURRENCY || 1);
const CALENDAR_DATA_SWEEP_PAGE_SIZE = Number(process.env.CALENDAR_DATA_SWEEP_PAGE_SIZE || 1);
const BACKGROUND_INITIAL_DELAY_MS = Number(process.env.BACKGROUND_INITIAL_DELAY_MS || 5000);
const BACKGROUND_REFRESH_COOLDOWN_MS = Number(process.env.BACKGROUND_REFRESH_COOLDOWN_MS || 10 * 60 * 1000);

let notionCallQueue = Promise.resolve();
let notionNextAllowedAt = 0;
let notionFailureTimestamps = [];
let notionCircuitOpenUntil = 0;

function isGatewayTimeoutError(error) {
  const message = String(error?.message || '').toLowerCase();
  return (
    error?.status === 504 ||
    message.includes('status: 504') ||
    message.includes('504 gateway') ||
    message.includes('gateway time-out') ||
    message.includes('gateway timeout')
  );
}

function pruneNotionFailureWindow(nowTs) {
  const minTs = nowTs - NOTION_CIRCUIT_WINDOW_MS;
  notionFailureTimestamps = notionFailureTimestamps.filter(ts => ts >= minTs);
}

function isNotionCircuitOpen() {
  return notionCircuitOpenUntil > Date.now();
}

function markNotionCallSuccess() {
  const nowTs = Date.now();
  pruneNotionFailureWindow(nowTs);
  notionFailureTimestamps = [];
  if (notionCircuitOpenUntil <= nowTs) {
    notionCircuitOpenUntil = 0;
  }
}

function markNotionCallFailure(error) {
  if (!isRetryableNotionError(error)) return;
  const nowTs = Date.now();
  pruneNotionFailureWindow(nowTs);
  notionFailureTimestamps.push(nowTs);
  if (notionFailureTimestamps.length >= NOTION_CIRCUIT_FAILURE_THRESHOLD) {
    const nextOpenUntil = nowTs + NOTION_CIRCUIT_COOLDOWN_MS;
    if (nextOpenUntil > notionCircuitOpenUntil) {
      notionCircuitOpenUntil = nextOpenUntil;
      console.warn(
        `🚨 Notion circuit opened for ${NOTION_CIRCUIT_COOLDOWN_MS}ms after ${notionFailureTimestamps.length} failures in ${NOTION_CIRCUIT_WINDOW_MS}ms`
      );
    }
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const GOOGLE_CALENDAR_TIMEZONE = 'America/Los_Angeles';
const GOOGLE_VTIMEZONE_BLOCK = [
  'BEGIN:VTIMEZONE',
  `TZID:${GOOGLE_CALENDAR_TIMEZONE}`,
  `X-LIC-LOCATION:${GOOGLE_CALENDAR_TIMEZONE}`,
  'BEGIN:DAYLIGHT',
  'TZOFFSETFROM:-0800',
  'TZOFFSETTO:-0700',
  'TZNAME:PDT',
  'DTSTART:19700308T020000',
  'RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU',
  'END:DAYLIGHT',
  'BEGIN:STANDARD',
  'TZOFFSETFROM:-0700',
  'TZOFFSETTO:-0800',
  'TZNAME:PST',
  'DTSTART:19701101T020000',
  'RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU',
  'END:STANDARD',
  'END:VTIMEZONE'
].join('\r\n');

function addCalendarTimezoneMetadata(icsData) {
  if (typeof icsData !== 'string' || icsData.includes('X-WR-TIMEZONE:')) {
    return icsData;
  }

  return icsData.replace(
    /(VERSION:2\.0\r?\n)/,
    `$1X-WR-TIMEZONE:${GOOGLE_CALENDAR_TIMEZONE}\r\n`
  );
}

function serializeCalendar(calendar) {
  return addCalendarTimezoneMetadata(calendar.toString());
}

function serializeGoogleCalendar(calendar) {
  if (!calendar) {
    return '';
  }

  let icsData = addCalendarTimezoneMetadata(calendar.toString());

  icsData = icsData
    .replace(/\r?\nDTSTART:(\d{8}T\d{6})/g, `\r\nDTSTART;TZID=${GOOGLE_CALENDAR_TIMEZONE}:$1`)
    .replace(/\r?\nDTEND:(\d{8}T\d{6})/g, `\r\nDTEND;TZID=${GOOGLE_CALENDAR_TIMEZONE}:$1`);

  if (!icsData.includes('BEGIN:VTIMEZONE')) {
    icsData = icsData.replace(
      /(X-WR-TIMEZONE:[^\r\n]+\r?\n)/,
      `$1${GOOGLE_VTIMEZONE_BLOCK}\r\n`
    );
  }

  return icsData;
}

function getRetryAfterMs(error) {
  const rawHeader = error?.headers?.['retry-after']
    || error?.headers?.['Retry-After']
    || error?.response?.headers?.['retry-after']
    || error?.response?.headers?.['Retry-After'];
  if (!rawHeader) return null;
  const parsed = Number(rawHeader);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return parsed * 1000;
}

function isRetryableNotionError(error) {
  const message = error?.message || '';
  return error?.code === 'notionhq_client_request_timeout' ||
         error?.status === 429 ||
         error?.status === 502 ||
         error?.status === 503 ||
         error?.status === 504 ||
         message.includes('504') ||
         message.includes('503') ||
         message.includes('502') ||
         message.includes('429') ||
         message.includes('timeout') ||
         message.includes('ECONNRESET') ||
         message.includes('EAI_AGAIN') ||
         message.includes('Request to Notion API failed');
}

async function runRateLimitedNotionCall(apiCall) {
  notionCallQueue = notionCallQueue.then(async () => {
    const waitMs = Math.max(0, notionNextAllowedAt - Date.now());
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    notionNextAllowedAt = Date.now() + NOTION_MIN_INTERVAL_MS;
  });

  await notionCallQueue;
  return apiCall();
}

// Helper function to retry Notion API calls with exponential backoff + jitter.
// Honors Retry-After for 429s when present.
async function retryNotionCall(apiCall, maxRetries = 5) {
  let lastError;
  const opStart = Date.now();
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    if (isNotionCircuitOpen()) {
      const waitMs = Math.max(0, notionCircuitOpenUntil - Date.now());
      const circuitError = new Error(`Notion circuit breaker open; retry after ${waitMs}ms`);
      circuitError.code = 'notion_circuit_open';
      throw circuitError;
    }
    try {
      const result = await runRateLimitedNotionCall(apiCall);
      markNotionCallSuccess();
      return result;
    } catch (error) {
      lastError = error;
      markNotionCallFailure(error);
      const elapsedMs = Date.now() - opStart;
      const isRetryable = isRetryableNotionError(error);

      if (!isRetryable || attempt >= maxRetries || elapsedMs >= NOTION_MAX_RETRY_BUDGET_MS) {
        throw error;
      }

      if (isNotionCircuitOpen()) {
        throw error;
      }

      const retryAfterMs = getRetryAfterMs(error);
      const baseBackoffMs = Math.min(1000 * Math.pow(2, attempt - 1), 30000);
      const jitterMs = Math.floor(Math.random() * 500);
      const delay = Math.max(retryAfterMs || 0, baseBackoffMs + jitterMs);
      console.log(`⚠️  Notion retryable error (attempt ${attempt}/${maxRetries}), retrying in ${delay}ms...`);
      await sleep(delay);
    }
  }
  
  throw lastError;
}

async function fetchAllDatabasePages(databaseId, options = {}) {
  const {
    pageSize = 100,
    filter,
    sorts,
    maxRetries = 5
  } = options;

  const results = [];
  let hasMore = true;
  let cursor = undefined;
  let pageNum = 0;

  if (databaseId === PERSONNEL_DB) {
    // #region agent log
    fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'bg_refresh_trace',hypothesisId:'H2',location:'index.js:fetchAllDatabasePages:entry',message:'Personnel pagination query started',data:{databaseId,pageSize,hasFilter:!!filter,hasSorts:!!sorts},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
  }

  while (hasMore) {
    pageNum += 1;
    const queryParams = {
      database_id: databaseId,
      page_size: pageSize
    };
    if (filter) queryParams.filter = filter;
    if (sorts) queryParams.sorts = sorts;
    if (cursor) queryParams.start_cursor = cursor;

    const response = await retryNotionCall(() => notion.databases.query(queryParams), maxRetries);
    results.push(...(response.results || []));
    hasMore = !!response.has_more;
    cursor = response.next_cursor || undefined;

    if (databaseId === PERSONNEL_DB) {
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'bg_refresh_trace',hypothesisId:'H2',location:'index.js:fetchAllDatabasePages:page',message:'Personnel pagination page fetched',data:{pageNum,pageSize,returned:(response.results||[]).length,hasMore,totalAccumulated:results.length},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
    }
  }

  return results;
}

async function mapWithConcurrency(items, concurrency, worker) {
  const safeConcurrency = Math.max(1, Math.min(concurrency || 1, items.length || 1));
  const results = new Array(items.length);
  let currentIndex = 0;

  const runners = Array.from({ length: safeConcurrency }, async () => {
    while (true) {
      const idx = currentIndex++;
      if (idx >= items.length) return;
      results[idx] = await worker(items[idx], idx);
    }
  });

  await Promise.all(runners);
  return results;
}

const REGEN_TOTAL_TIMEOUT_MS = Number(process.env.REGEN_TOTAL_TIMEOUT_MS || 60000); // hard per-person budget
const REGEN_FETCH_STEP_TIMEOUT_MS = Number(process.env.REGEN_FETCH_STEP_TIMEOUT_MS || 60000);
const REGEN_PERSON_STEP_TIMEOUT_MS = Number(process.env.REGEN_PERSON_STEP_TIMEOUT_MS || 60000);
const CALENDAR_FETCH_TIMEOUT_MS = Number(process.env.CALENDAR_FETCH_TIMEOUT_MS || 60000); // max allowed for admin/travel/blockout pulls
const REGEN_MODE_FULL = 'full';
const REGEN_MODE_EVENTS_ONLY = 'events_only';
const REGEN_MODE_NON_EVENTS_ONLY = 'non_events_only';
const SPLIT_REGEN_TEST_PERSON_ID = normalizeNotionPageId(
  process.env.SPLIT_REGEN_TEST_PERSON_ID || 'ac294b7c-1907-4977-b5ba-191890a397a3'
);

function parseRegenMode(rawMode) {
  if (rawMode === undefined || rawMode === null || rawMode === '') {
    return REGEN_MODE_FULL;
  }

  const value = String(rawMode).trim().toLowerCase();
  if (value === REGEN_MODE_FULL) return REGEN_MODE_FULL;

  if (
    value === REGEN_MODE_EVENTS_ONLY ||
    value === 'events' ||
    value === 'main' ||
    value === 'main_only'
  ) {
    return REGEN_MODE_EVENTS_ONLY;
  }

  if (
    value === REGEN_MODE_NON_EVENTS_ONLY ||
    value === 'non_events' ||
    value === 'aux' ||
    value === 'aux_only' ||
    value === 'other_properties'
  ) {
    return REGEN_MODE_NON_EVENTS_ONLY;
  }

  return null;
}

function isSplitRegenTestPerson(personId) {
  return normalizeNotionPageId(personId) === SPLIT_REGEN_TEST_PERSON_ID;
}

function isSplitModeAllowedForPerson(personId, regenMode) {
  return regenMode === REGEN_MODE_FULL || isSplitRegenTestPerson(personId);
}

function buildCalendarCacheKey(personId, formatKey, regenMode = REGEN_MODE_FULL) {
  if (regenMode === REGEN_MODE_FULL) {
    return `calendar:${personId}:${formatKey}`;
  }
  return `calendar:${personId}:${regenMode}:${formatKey}`;
}

function withTimeout(promise, timeoutMs, timeoutMessage) {
  if (!timeoutMs || timeoutMs <= 0) {
    return Promise.reject(new Error(timeoutMessage));
  }
  let timer = null;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function getRemainingMs(deadlineTs) {
  return deadlineTs - Date.now();
}

let calendarDataPropertyIdCache = null;

async function getCalendarDataPropertyIdMap(maxRetries = 5) {
  if (calendarDataPropertyIdCache) {
    return calendarDataPropertyIdCache;
  }

  const dbInfo = await retryNotionCall(
    () => notion.databases.retrieve({ database_id: CALENDAR_DATA_DB }),
    maxRetries
  );
  const props = dbInfo?.properties || {};

  calendarDataPropertyIdCache = {
    Name: props.Name?.id || null,
    Personnel: props.Personnel?.id || null,
    Events: props.Events?.id || null,
    Flights: props.Flights?.id || null,
    Transportation: props.Transportation?.id || null,
    Hotels: props.Hotels?.id || null,
    Rehearsals: props.Rehearsals?.id || null,
    TeamCalendar: props['Team Calendar']?.id || null,
    EventNotesReminders: props['Event Notes Reminders']?.id || null,
  };
  return calendarDataPropertyIdCache;
}

function extractPropertyStringFromItem(item) {
  if (!item || typeof item !== 'object') return '';

  if (item.type === 'formula') {
    const formula = item.formula;
    if (formula?.type === 'string') return formula.string || '';
    if (formula?.type === 'number') return formula.number != null ? String(formula.number) : '';
    if (formula?.type === 'boolean') return formula.boolean != null ? String(formula.boolean) : '';
    if (formula?.type === 'date') {
      if (!formula.date) return '';
      const start = formula.date.start || '';
      const end = formula.date.end || '';
      return end ? `${start}/${end}` : start;
    }
    return '';
  }

  if (item.type === 'rich_text' && Array.isArray(item.rich_text)) {
    return item.rich_text.map(t => t?.plain_text || '').join('');
  }

  if (item.type === 'title' && Array.isArray(item.title)) {
    return item.title.map(t => t?.plain_text || '').join('');
  }

  return '';
}

async function fetchPagePropertyString(pageId, propertyId, maxRetries = 5) {
  if (!propertyId) return '';

  const first = await retryNotionCall(
    () => notion.pages.properties.retrieve({ page_id: pageId, property_id: propertyId }),
    maxRetries
  );

  if (first?.object !== 'list') {
    return extractPropertyStringFromItem(first);
  }

  let text = (first.results || []).map(extractPropertyStringFromItem).join('');
  let cursor = first.next_cursor || null;
  let hasMore = !!first.has_more;

  while (hasMore && cursor) {
    const next = await retryNotionCall(
      () => notion.pages.properties.retrieve({
        page_id: pageId,
        property_id: propertyId,
        start_cursor: cursor,
      }),
      maxRetries
    );
    text += (next.results || []).map(extractPropertyStringFromItem).join('');
    cursor = next.next_cursor || null;
    hasMore = !!next.has_more;
  }

  return text;
}

async function getCalendarDataPagePropertiesLean(pageId, maxRetries = 5) {
  const ids = await getCalendarDataPropertyIdMap(maxRetries);

  const eventsStr = await fetchPagePropertyString(pageId, ids.Events, maxRetries);
  const flightsStr = await fetchPagePropertyString(pageId, ids.Flights, maxRetries);
  const transportationStr = await fetchPagePropertyString(pageId, ids.Transportation, maxRetries);
  const hotelsStr = await fetchPagePropertyString(pageId, ids.Hotels, maxRetries);
  const rehearsalsStr = await fetchPagePropertyString(pageId, ids.Rehearsals, maxRetries);
  const teamCalendarStr = await fetchPagePropertyString(pageId, ids.TeamCalendar, maxRetries);
  const eventNotesRemindersStr = await fetchPagePropertyString(pageId, ids.EventNotesReminders, maxRetries);

  return {
    Events: { formula: { string: eventsStr || '[]' } },
    Flights: { formula: { string: flightsStr || '[]' } },
    Transportation: { formula: { string: transportationStr || '[]' } },
    Hotels: { formula: { string: hotelsStr || '[]' } },
    Rehearsals: { formula: { string: rehearsalsStr || '[]' } },
    'Team Calendar': { formula: { string: teamCalendarStr || '[]' } },
    'Event Notes Reminders': { formula: { string: eventNotesRemindersStr || '[]' } },
  };
}

async function getCalendarDataFromPage(page, maxRetries = 5) {
  const pageId = page?.id || null;
  const pageProperties = page?.properties || {};
  const linkedPersonId = getPrimaryRelationPageId(pageProperties.Personnel);

  if (!pageId) {
    throw new Error('Calendar Data page is missing an id');
  }

  if (hasInlineCalendarDataFormulaStrings(pageProperties)) {
    try {
      return {
        pageId,
        linkedPersonId,
        calendarData: processCalendarDataProperties(pageProperties),
        source: 'page_payload'
      };
    } catch (error) {
      console.warn(`⚠️  Direct Calendar Data payload parse failed for ${pageId}, retrying via property fetch: ${error.message}`);
    }
  }

  const leanProperties = await getCalendarDataPagePropertiesLean(pageId, maxRetries);
  return {
    pageId,
    linkedPersonId,
    calendarData: processCalendarDataProperties({
      ...pageProperties,
      ...leanProperties
    }),
    source: 'page_properties'
  };
}

function normalizeNotionPageId(input) {
  if (!input || typeof input !== 'string') return null;
  const trimmed = input.trim();
  if (!trimmed) return null;

  const directMatch = trimmed.match(/[0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/);
  const rawId = directMatch ? directMatch[0] : null;
  if (!rawId) return null;

  return rawId.includes('-')
    ? rawId.toLowerCase()
    : rawId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5').toLowerCase();
}

function getPrimaryRelationPageId(propertyValue) {
  return Array.isArray(propertyValue?.relation) && propertyValue.relation.length > 0
    ? propertyValue.relation[0].id
    : null;
}

function hasInlineCalendarDataFormulaStrings(calendarDataProperties) {
  const requiredKeys = [
    'Events',
    'Flights',
    'Transportation',
    'Hotels',
    'Rehearsals',
    'Team Calendar',
    'Event Notes Reminders'
  ];

  return requiredKeys.every(key => {
    const formula = calendarDataProperties?.[key]?.formula;
    return formula?.type === 'string' && typeof formula.string === 'string';
  });
}

async function getCalendarDataFromPageIdOrUrl(calendarDataInput, maxRetries = 5) {
  const pageId = normalizeNotionPageId(calendarDataInput);
  if (!pageId) {
    throw new Error('Invalid Calendar Data page ID or URL');
  }

  const page = await retryNotionCall(
    () => notion.pages.retrieve({ page_id: pageId }),
    maxRetries
  );

  return getCalendarDataFromPage(page, maxRetries);
}

async function getCalendarDataEventsOnlyFromPageIdOrUrl(calendarDataInput, maxRetries = 5) {
  const pageId = normalizeNotionPageId(calendarDataInput);
  if (!pageId) {
    throw new Error('Invalid Calendar Data page ID or URL');
  }

  const page = await retryNotionCall(
    () => notion.pages.retrieve({ page_id: pageId }),
    maxRetries
  );

  const linkedPersonId = getPrimaryRelationPageId(page?.properties?.Personnel);
  const propertyIds = await getCalendarDataPropertyIdMap(maxRetries);
  const eventsString = await fetchPagePropertyString(pageId, propertyIds.Events, maxRetries);
  const events = parseJsonFormulaArray({ formula: { string: eventsString || '[]' } }, 'Events');

  return {
    pageId,
    linkedPersonId,
    calendarData: {
      personName: extractPropertyStringFromItem(page?.properties?.Name) || 'Unknown',
      events: Array.isArray(events) ? events : [],
      flights: [],
      rehearsals: [],
      hotels: [],
      ground_transport: [],
      team_calendar: [],
      event_note_reminders: []
    },
    source: 'page_events_property_only'
  };
}

async function getCalendarDataFromDatabaseQueryStyle(personId, maxRetries = 5) {
  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
  }

  const queryStart = Date.now();
  const response = await retryNotionCall(() =>
    notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      page_size: 1,
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    }),
    maxRetries
  );
  console.log(`📊 CalendarData single-query timing for ${personId}: ${Date.now() - queryStart}ms`);

  return processCalendarDataResponse(response);
}

function parseJsonFormulaArray(propertyValue, propertyLabel) {
  const raw = propertyValue?.formula?.string ?? extractPropertyStringFromItem(propertyValue) ?? '[]';
  try {
    const parsed = JSON.parse(raw || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    console.error(`Error parsing ${propertyLabel} JSON:`, raw?.substring?.(0, 100) || '');
    throw new Error(`${propertyLabel} JSON parse error: ${e.message}`);
  }
}

function extractPersonNameFromPersonnelPage(personPage) {
  if (!personPage?.properties) return 'Unknown';
  return (
    extractPropertyStringFromItem(personPage.properties['Full Name']) ||
    extractPropertyStringFromItem(personPage.properties.Name) ||
    'Unknown'
  );
}

async function getCalendarDataEventsOnlyFromPersonRelationPath(personId, maxRetries = 5) {
  const personFetchStart = Date.now();
  const personPage = await retryNotionCall(() => notion.pages.retrieve({ page_id: personId }), maxRetries);
  const personFetchMs = Date.now() - personFetchStart;

  const calendarDataPageId = getPrimaryRelationPageId(personPage?.properties?.['Calendar Data']);
  if (!calendarDataPageId) {
    console.warn(`⚠️  No linked Calendar Data row found on Personnel page for ${personId}`);
    return null;
  }

  const propertyIds = await getCalendarDataPropertyIdMap(maxRetries);
  const eventsFetchStart = Date.now();
  const eventsString = await fetchPagePropertyString(calendarDataPageId, propertyIds.Events, maxRetries);
  const eventsFetchMs = Date.now() - eventsFetchStart;
  const events = parseJsonFormulaArray({ formula: { string: eventsString || '[]' } }, 'Events');

  console.log(`📊 CalendarData events-only direct timings for ${personId}: person=${personFetchMs}ms eventsProperty=${eventsFetchMs}ms`);

  return {
    personName: extractPersonNameFromPersonnelPage(personPage),
    events: Array.isArray(events) ? events : [],
    flights: [],
    rehearsals: [],
    hotels: [],
    ground_transport: [],
    team_calendar: [],
    event_note_reminders: []
  };
}

async function getCalendarDataNonEventsOnlyFromDatabaseQueryStyle(personId, maxRetries = 5) {
  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
  }

  const queryStart = Date.now();
  const propertyIds = await getCalendarDataPropertyIdMap(maxRetries);
  const response = await retryNotionCall(() => {
    const queryParams = {
      database_id: CALENDAR_DATA_DB,
      page_size: 1,
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    };
    const filterProperties = [
      propertyIds.Name,
      propertyIds.Flights,
      propertyIds.Transportation,
      propertyIds.Hotels,
      propertyIds.Rehearsals,
      propertyIds.TeamCalendar,
      propertyIds.EventNotesReminders
    ].filter(Boolean);
    if (filterProperties.length > 0) {
      queryParams.filter_properties = filterProperties;
    }
    return notion.databases.query(queryParams);
  }, maxRetries);
  console.log(`📊 CalendarData non-events query timing for ${personId}: ${Date.now() - queryStart}ms`);

  if (response.results.length === 0) {
    return null;
  }

  const props = response.results[0].properties || {};
  const personName = extractPropertyStringFromItem(props.Name) || 'Unknown';

  return {
    personName,
    events: [],
    flights: parseJsonFormulaArray(props.Flights, 'Flights'),
    rehearsals: parseJsonFormulaArray(props.Rehearsals, 'Rehearsals'),
    hotels: parseJsonFormulaArray(props.Hotels, 'Hotels'),
    ground_transport: parseJsonFormulaArray(props.Transportation, 'Transportation'),
    team_calendar: parseJsonFormulaArray(props['Team Calendar'], 'Team Calendar'),
    event_note_reminders: parseJsonFormulaArray(props['Event Notes Reminders'], 'Event Notes Reminders')
  };
}

// Helper function to get calendar data from Calendar Data database
async function getCalendarDataFromDatabase(personId, options = {}) {
  const { maxRetries = 5, allowFallbackQuery = false } = options;
  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
  }

  // Fast path: read the person's direct "Calendar Data" relation and fetch that page.
  // This avoids relation-filter scans on the whole Calendar Data DB, which can 504 on large datasets.
  try {
    const fastPathStart = Date.now();
    const personPage = await retryNotionCall(() => notion.pages.retrieve({ page_id: personId }), maxRetries);
    const personFetchMs = Date.now() - fastPathStart;
    const calendarDataRelation = personPage?.properties?.['Calendar Data'];
    const calendarDataPageId = Array.isArray(calendarDataRelation?.relation) && calendarDataRelation.relation.length > 0
      ? calendarDataRelation.relation[0].id
      : null;

    if (calendarDataPageId) {
      const relationFetchStart = Date.now();
      const calendarDataProps = await getCalendarDataPagePropertiesLean(calendarDataPageId, maxRetries);
      const relationFetchMs = Date.now() - relationFetchStart;
      const processed = processCalendarDataProperties(calendarDataProps);
      if (processed) {
        console.log(`📊 CalendarData fast-path timings for ${personId}: person=${personFetchMs}ms relation=${relationFetchMs}ms`);
        return processed;
      }
    }
  } catch (directFetchError) {
    const shouldSkipFallback = isGatewayTimeoutError(directFetchError) || directFetchError?.code === 'notion_circuit_open';
    if (shouldSkipFallback) {
      console.warn(`⚠️  Direct Calendar Data page fetch failed for ${personId}; skipping DB fallback: ${directFetchError.message}`);
      throw directFetchError;
    }
    // Fall back to DB query mode below only for non-timeout errors.
    console.warn(`⚠️  Direct Calendar Data page fetch failed for ${personId}, considering DB fallback: ${directFetchError.message}`);
  }

  if (!(ENABLE_CALENDAR_DB_FALLBACK && allowFallbackQuery)) {
    console.warn(`⏭️  Calendar Data DB fallback disabled for ${personId}`);
    return null;
  }

  // Query Calendar Data database for events related to this person
  // Use page_size: 1 since we only expect one result per person
  const fallbackStart = Date.now();
  const response = await retryNotionCall(() => 
    notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      page_size: 1, // Optimize: only fetch one result
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    }),
    maxRetries
  );
  console.log(`📊 CalendarData fallback query timing for ${personId}: ${Date.now() - fallbackStart}ms`);
  
  return processCalendarDataResponse(response);
}

async function regenerateFromCalendarDataPage(calendarDataPage, options = {}) {
  const { trigger = 'unknown' } = options;
  const pageId = calendarDataPage?.id || 'unknown';

  try {
    const linkedPersonId = getPrimaryRelationPageId(calendarDataPage?.properties?.Personnel);
    if (!linkedPersonId) {
      console.warn(`⚠️  Calendar Data row ${pageId} is missing a Personnel relation`);
      return { success: false, pageId, reason: 'missing_personnel_relation' };
    }

    const result = await regenerateCalendarForPerson(linkedPersonId, { trigger });
    return { ...result, pageId };
  } catch (error) {
    console.error(`❌ Failed to process Calendar Data row ${pageId}:`, error.message);
    return { success: false, pageId, error: error.message };
  }
}

async function processCalendarDataIndexEntries(entries, options = {}) {
  const {
    trigger = 'unknown',
    concurrency = 1,
    waitContext = null,
    source = 'redis_index',
    pageCount = 0
  } = options;

  const normalizedEntries = normalizeCalendarDataIndexEntries(entries);
  const results = await mapWithConcurrency(normalizedEntries, concurrency, async (entry) => {
    if (waitContext) {
      await waitForManualRegensToDrain(waitContext);
    }
    const result = await regenerateCalendarForPerson(entry.personId, { trigger });
    return { ...result, pageId: entry.pageId };
  });

  console.log(
    `📚 Processed Calendar Data ${source}: rows=${normalizedEntries.length}, pageCount=${pageCount}, concurrency=${concurrency}`
  );

  return {
    results,
    totalRows: normalizedEntries.length,
    pageCount
  };
}

async function extendCalendarDataRowIndex(options = {}) {
  const {
    maxRetries = 5,
    pageSize = CALENDAR_DATA_SWEEP_PAGE_SIZE,
    pagesPerRun = CALENDAR_DATA_INDEX_BUILD_PAGES_PER_CYCLE
  } = options;

  if (!redis || !cacheEnabled) {
    return null;
  }

  const existingState = await getCachedJson(CALENDAR_DATA_INDEX_STATE_CACHE_KEY);
  const state = existingState && typeof existingState === 'object'
    ? existingState
    : {
        entries: [],
        nextCursor: null,
        pageCount: 0,
        startedAt: new Date().toISOString()
      };

  state.entries = normalizeCalendarDataIndexEntries(state.entries);

  const propertyIds = await getCalendarDataPropertyIdMap(maxRetries);
  const discoveryPropertyIds = [propertyIds.Name, propertyIds.Personnel].filter(Boolean);

  let cursor = state.nextCursor || undefined;
  let hasMore = true;
  let pagesFetched = 0;

  while (hasMore && pagesFetched < pagesPerRun) {
    const queryParams = {
      database_id: CALENDAR_DATA_DB,
      page_size: pageSize
    };
    if (discoveryPropertyIds.length > 0) {
      queryParams.filter_properties = discoveryPropertyIds;
    }
    if (cursor) {
      queryParams.start_cursor = cursor;
    }

    const response = await retryNotionCall(() => notion.databases.query(queryParams), maxRetries);
    const rows = response.results || [];
    const entries = rows
      .map((row) => ({
        pageId: row.id,
        personId: getPrimaryRelationPageId(row.properties?.Personnel)
      }))
      .filter((entry) => entry.personId);

    state.entries.push(...entries);
    state.entries = normalizeCalendarDataIndexEntries(state.entries);
    state.pageCount += 1;
    pagesFetched += 1;
    hasMore = !!response.has_more;
    cursor = response.next_cursor || undefined;

    console.log(
      `🧱 Calendar Data index batch ${state.pageCount}: rows=${rows.length}, indexed=${state.entries.length}, hasMore=${hasMore}`
    );
  }

  state.nextCursor = hasMore ? (cursor || null) : null;
  state.updatedAt = new Date().toISOString();

  if (!hasMore) {
    const completeIndex = {
      entries: state.entries,
      pageCount: state.pageCount,
      generatedAt: state.updatedAt
    };
    await setCachedJson(CALENDAR_DATA_INDEX_CACHE_KEY, completeIndex, CALENDAR_DATA_INDEX_TTL);
    await redis.del(CALENDAR_DATA_INDEX_STATE_CACHE_KEY);
    return {
      ...completeIndex,
      complete: true,
      source: 'redis_index'
    };
  }

  await setCachedJson(CALENDAR_DATA_INDEX_STATE_CACHE_KEY, state, CALENDAR_DATA_INDEX_TTL);
  return {
    entries: state.entries,
    pageCount: state.pageCount,
    generatedAt: state.updatedAt,
    complete: false,
    source: 'redis_index_partial'
  };
}

async function processCalendarDataRowsPaginated(options = {}) {
  const {
    trigger = 'unknown',
    concurrency = 1,
    maxRetries = 5,
    pageSize = CALENDAR_DATA_SWEEP_PAGE_SIZE,
    waitContext = null
  } = options;

  if (!CALENDAR_DATA_DB) {
    throw new Error('CALENDAR_DATA_DATABASE_ID not configured');
  }

  if (redis && cacheEnabled) {
    const cachedIndex = await getCachedJson(CALENDAR_DATA_INDEX_CACHE_KEY);
    const cachedEntries = normalizeCalendarDataIndexEntries(cachedIndex?.entries);
    if (cachedEntries.length > 0) {
      console.log(`📚 Using cached Calendar Data index with ${cachedEntries.length} rows`);
      return processCalendarDataIndexEntries(cachedEntries, {
        trigger,
        concurrency,
        waitContext,
        source: 'redis_index',
        pageCount: Number(cachedIndex?.pageCount) || 0
      });
    }

    try {
      const builtIndex = await extendCalendarDataRowIndex({ maxRetries, pageSize });
      const indexedEntries = normalizeCalendarDataIndexEntries(builtIndex?.entries);
      if (indexedEntries.length > 0) {
        console.log(
          `${builtIndex.complete ? '✅' : '🧱'} Calendar Data index ${builtIndex.complete ? 'ready' : 'extended'} with ${indexedEntries.length} rows`
        );
        return processCalendarDataIndexEntries(indexedEntries, {
          trigger,
          concurrency,
          waitContext,
          source: builtIndex.source,
          pageCount: Number(builtIndex?.pageCount) || 0
        });
      }
    } catch (indexError) {
      console.warn(`⚠️  Calendar Data Redis index build failed, falling back to live sweep: ${indexError.message}`);
    }
  }

  const results = [];
  let totalRows = 0;
  let pageCount = 0;
  let hasMore = true;
  let cursor = undefined;
  const propertyIds = await getCalendarDataPropertyIdMap(maxRetries);
  const discoveryPropertyIds = [propertyIds.Name, propertyIds.Personnel].filter(Boolean);

  while (hasMore) {
    pageCount += 1;
    const batchStart = Date.now();
    const queryParams = {
      database_id: CALENDAR_DATA_DB,
      page_size: pageSize
    };
    if (discoveryPropertyIds.length > 0) {
      queryParams.filter_properties = discoveryPropertyIds;
    }
    if (cursor) {
      queryParams.start_cursor = cursor;
    }

    const response = await retryNotionCall(() => notion.databases.query(queryParams), maxRetries);
    const rows = response.results || [];
    totalRows += rows.length;

    console.log(
      `📄 Calendar Data batch ${pageCount}: rows=${rows.length}, total=${totalRows}, fetchMs=${Date.now() - batchStart}, hasMore=${!!response.has_more}`
    );

    const batchResults = await mapWithConcurrency(rows, concurrency, async (calendarDataPage) => {
      if (waitContext) {
        await waitForManualRegensToDrain(waitContext);
      }
      return regenerateFromCalendarDataPage(calendarDataPage, { trigger, maxRetries });
    });
    results.push(...batchResults);

    hasMore = !!response.has_more;
    cursor = response.next_cursor || undefined;
  }

  return { results, totalRows, pageCount };
}

// Helper function to process the response after successful query
function processCalendarDataResponse(response) {
  if (response.results.length === 0) {
    return null;
  }

  return processCalendarDataProperties(response.results[0].properties);
}

function processCalendarDataProperties(calendarData) {
  if (!calendarData) {
    return null;
  }

  
  // Parse all the JSON strings with better error handling
  let events, flights, transportation, hotels, rehearsals, teamCalendar, eventNotesReminders;
  
  const rawFormulaString = calendarData.Events?.formula?.string || '';
  
  try {
    events = JSON.parse(rawFormulaString || '[]');
  } catch (e) {
    console.error('Error parsing Events JSON:', calendarData.Events?.formula?.string?.substring(0, 100));
    throw new Error(`Events JSON parse error: ${e.message}`);
  }
  
  try {
    flights = JSON.parse(calendarData.Flights?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Flights JSON:', calendarData.Flights?.formula?.string?.substring(0, 100));
    throw new Error(`Flights JSON parse error: ${e.message}`);
  }
  
  try {
    transportation = JSON.parse(calendarData.Transportation?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Transportation JSON:', calendarData.Transportation?.formula?.string?.substring(0, 100));
    throw new Error(`Transportation JSON parse error: ${e.message}`);
  }
  
  try {
    hotels = JSON.parse(calendarData.Hotels?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Hotels JSON:', calendarData.Hotels?.formula?.string?.substring(0, 100));
    throw new Error(`Hotels JSON parse error: ${e.message}`);
  }
  
  try {
    rehearsals = JSON.parse(calendarData.Rehearsals?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Rehearsals JSON:', calendarData.Rehearsals?.formula?.string?.substring(0, 100));
    throw new Error(`Rehearsals JSON parse error: ${e.message}`);
  }
  
  try {
    teamCalendar = JSON.parse(calendarData['Team Calendar']?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Team Calendar JSON:', calendarData['Team Calendar']?.formula?.string?.substring(0, 100));
    throw new Error(`Team Calendar JSON parse error: ${e.message}`);
  }

  try {
    eventNotesReminders = JSON.parse(calendarData['Event Notes Reminders']?.formula?.string || '[]');
  } catch (e) {
    console.error('Error parsing Event Notes Reminders JSON:', calendarData['Event Notes Reminders']?.formula?.string?.substring(0, 100));
    throw new Error(`Event Notes Reminders JSON parse error: ${e.message}`);
  }

  // Normalize Team Calendar keys (handle variations like DCOS vs dcos, Title vs title, etc.)
  teamCalendar = teamCalendar.map(original => {
    const normalized = { ...original };

    Object.keys(original || {}).forEach(key => {
      const normalizedKey = key.toLowerCase().trim();
      const value = original[key];

      switch (normalizedKey) {
        case 'title':
          if (!normalized.title && typeof value === 'string') {
            normalized.title = value;
          }
          break;
        case 'address':
        case 'location':
          if (!normalized.address && typeof value === 'string') {
            normalized.address = value;
          }
          break;
        case 'notion_link':
        case 'notionlink':
        case 'link':
          if (!normalized.notion_link && typeof value === 'string') {
            normalized.notion_link = value;
          }
          break;
        case 'notes':
          if (normalized.notes === undefined) {
            normalized.notes = value ?? '';
          }
          break;
        case 'dcos':
        case 'dcos_text':
          if (!normalized.dcos && typeof value === 'string') {
            normalized.dcos = value;
          }
          break;
        case 'date':
        case 'date_range':
          if (!normalized.date && typeof value === 'string') {
            normalized.date = value;
          }
          break;
        default:
          break;
      }
    });

    normalized.notes = normalized.notes ?? '';
    normalized.dcos = normalized.dcos ?? '';

    return normalized;
  });

  eventNotesReminders = eventNotesReminders.map(original => {
    const normalized = { ...original };

    Object.keys(original || {}).forEach(key => {
      const normalizedKey = key.toLowerCase().trim();
      const value = original[key];

      switch (normalizedKey) {
        case 'description':
        case 'title':
          if (!normalized.description && typeof value === 'string') {
            normalized.description = value;
          }
          break;
        case 'event_name':
        case 'eventname':
        case 'event':
          if (!normalized.event_name && typeof value === 'string') {
            normalized.event_name = value;
          }
          break;
        case 'remind_date':
        case 'reminddate':
        case 'date':
          if (!normalized.remind_date && typeof value === 'string') {
            normalized.remind_date = value;
          }
          break;
        case 'notion_link':
        case 'notionlink':
        case 'link':
          if (!normalized.notion_link && typeof value === 'string') {
            normalized.notion_link = value;
          }
          break;
        default:
          break;
      }
    });

    normalized.description = normalized.description ?? '';

    return normalized;
  });

  const personName = extractPropertyStringFromItem(calendarData.Name) || 'Unknown';

  // Transform into the same format as the old system
  // Return events with shared flights, hotels, rehearsals, and transportation
  return {
    personName,
    events: events.map(event => ({
      event_name: event.event_name,
      event_date: event.event_date,
      event_date_helper: event.event_date_helper,
      band: event.band,
      calltime: event.calltime,
      gear_checklist: event.gear_checklist,
      event_personnel: event.event_personnel,
      general_info: event.general_info,
      venue: event.venue,
      venue_address: event.venue_address,
      notion_url: event.notion_url,
      pay_total: event.pay_total,
      position: event.position,
      assignments: event.assignments,
      // Don't nest these - they will be added at the top level
      flights: [],
      rehearsals: [],
      hotels: [],
      ground_transport: []
    })),
    // Add shared data at the top level
    flights: flights,
    rehearsals: rehearsals,
    hotels: hotels,
    ground_transport: transportation,
    team_calendar: teamCalendar,
    event_note_reminders: eventNotesReminders
  };
}

function coerceCalendarDataToEventsOnly(calendarData) {
  if (!calendarData) return calendarData;

  const eventsArray = Array.isArray(calendarData.events) ? calendarData.events : [];
  const mainOnlyEvents = eventsArray.map(event => ({
    ...event,
    flights: [],
    hotels: [],
    ground_transport: [],
    rehearsal: [],
    rehearsals: []
  }));

  return {
    ...calendarData,
    events: mainOnlyEvents,
    flights: [],
    rehearsals: [],
    hotels: [],
    ground_transport: [],
    team_calendar: [],
    event_note_reminders: []
  };
}

function coerceCalendarDataToNonEventsOnly(calendarData) {
  if (!calendarData) return calendarData;

  return {
    ...calendarData,
    events: [],
    flights: Array.isArray(calendarData.flights) ? calendarData.flights : [],
    rehearsals: Array.isArray(calendarData.rehearsals) ? calendarData.rehearsals : [],
    hotels: Array.isArray(calendarData.hotels) ? calendarData.hotels : [],
    ground_transport: Array.isArray(calendarData.ground_transport) ? calendarData.ground_transport : [],
    team_calendar: Array.isArray(calendarData.team_calendar) ? calendarData.team_calendar : [],
    event_note_reminders: Array.isArray(calendarData.event_note_reminders) ? calendarData.event_note_reminders : []
  };
}

// Extract year/month/day/hours/minutes/seconds directly from an ISO string's local components.
// For "2026-02-21T07:00:00-08:00" → { year:2026, month:1, day:21, hours:7, minutes:0, seconds:0 }
// The -08:00 offset is ignored — we want the LOCAL time as written in the string.
function extractLocalComponents(isoStr) {
  const match = isoStr.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/);
  if (!match) return null;
  return {
    year: parseInt(match[1]),
    month: parseInt(match[2]) - 1, // 0-indexed for Date.UTC
    day: parseInt(match[3]),
    hours: parseInt(match[4]),
    minutes: parseInt(match[5]),
    seconds: parseInt(match[6])
  };
}

const MONTH_NAME_TO_INDEX = {
  january: 0,
  february: 1,
  march: 2,
  april: 3,
  may: 4,
  june: 5,
  july: 6,
  august: 7,
  september: 8,
  october: 9,
  november: 10,
  december: 11
};

function parseHumanDateAsFloating(dateStr) {
  if (typeof dateStr !== 'string') return null;
  const normalizedDate = dateStr
    .replace(/\u00A0/g, ' ')
    .replace(/\u202F/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const match = normalizedDate.match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
  if (!match) return null;
  const monthIndex = MONTH_NAME_TO_INDEX[match[1].toLowerCase()];
  if (monthIndex === undefined) return null;
  const day = parseInt(match[2], 10);
  const year = parseInt(match[3], 10);
  return new Date(Date.UTC(year, monthIndex, day, 0, 0, 0));
}

function parseHumanDateTimeAsFloating(dateStr, timeStr) {
  if (typeof timeStr !== 'string') return null;
  const date = parseHumanDateAsFloating(dateStr);
  if (!date) return null;
  const normalizedTime = timeStr
    .replace(/\u00A0/g, ' ')
    .replace(/\u202F/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const timeMatch = normalizedTime.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!timeMatch) return null;

  let hours = parseInt(timeMatch[1], 10) % 12;
  const minutes = parseInt(timeMatch[2], 10);
  const meridiem = timeMatch[3].toUpperCase();
  if (meridiem === 'PM') hours += 12;

  return new Date(Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    hours,
    minutes,
    0
  ));
}

function parseAtHumanRangeNoConversion(rawStr, options = {}) {
  if (typeof rawStr !== 'string') return null;
  const includeMeta = options.includeMeta === true;
  const withBranch = (value, branch) => {
    if (!value) return value;
    if (!includeMeta) return value;
    return { ...value, __branch: branch };
  };

  const clean = rawStr
    .replace(/[']/g, '')
    .replace(/\u00A0/g, ' ')
    .replace(/\u202F/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!clean.startsWith('@')) return null;
  const payload = clean.slice(1).trim();

  // Date-only all-day style: "Month D, YYYY → Month D, YYYY"
  const dateOnlyMatch = payload.match(/^([A-Za-z]+\s+\d{1,2},\s+\d{4})\s*(?:→|->)\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})$/i);
  if (dateOnlyMatch) {
    const startDate = parseHumanDateAsFloating(dateOnlyMatch[1]);
    const endDate = parseHumanDateAsFloating(dateOnlyMatch[2]);
    if (startDate && endDate && !isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
      return withBranch({ start: startDate, end: endDate }, 'at_date_only_range');
    }
    return null;
  }

  // Date/time range:
  // "Month D, YYYY h:mm AM → h:mm PM"
  // "Month D, YYYY h:mm AM → Month D, YYYY h:mm PM"
  // Optional timezone labels in parentheses are ignored.
  const rangeMatch = payload.match(
    /^([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(\d{1,2}:\d{2}\s*(?:AM|PM))(?:\s+\([^)]+\))?\s*(?:→|->)\s*(?:([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+)?(\d{1,2}:\d{2}\s*(?:AM|PM))(?:\s+\([^)]+\))?$/i
  );
  if (rangeMatch) {
    const startDateStr = rangeMatch[1].trim();
    const startTimeStr = rangeMatch[2].trim();
    const endDateStr = (rangeMatch[3] || startDateStr).trim();
    const endTimeStr = rangeMatch[4].trim();

    const startDate = parseHumanDateTimeAsFloating(startDateStr, startTimeStr);
    const endDate = parseHumanDateTimeAsFloating(endDateStr, endTimeStr);
    if (!startDate || !endDate || isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return null;
    }

    return withBranch({
      start: startDate,
      end: startDate.getTime() > endDate.getTime()
        ? new Date(endDate.getTime() + MAIN_EVENT_MS_PER_DAY)
        : endDate
    }, 'at_time_range');
  }

  // Single timestamp: "@Month D, YYYY h:mm AM"
  const singleMatch = payload.match(/^([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(\d{1,2}:\d{2}\s*(?:AM|PM))(?:\s+\([^)]+\))?$/i);
  if (singleMatch) {
    const date = parseHumanDateTimeAsFloating(singleMatch[1].trim(), singleMatch[2].trim());
    if (date && !isNaN(date.getTime())) {
      return withBranch({ start: date, end: date }, 'at_single_time');
    }
  }

  return null;
}

const MAIN_EVENT_MAX_DURATION_HOURS = 16;
const MAIN_EVENT_MS_PER_HOUR = 60 * 60 * 1000;
const MAIN_EVENT_MS_PER_DAY = 24 * MAIN_EVENT_MS_PER_HOUR;

function hasExplicitOffset(dateStr) {
  if (typeof dateStr !== 'string') return false;
  return /(?:Z|[+\-]\d{2}:\d{2})$/i.test(dateStr.trim());
}

function isDateOnlyString(dateStr) {
  if (typeof dateStr !== 'string') return false;
  return /^\d{4}-\d{2}-\d{2}$/.test(dateStr.trim());
}

function parseDateOnlyAsFloating(dateStr) {
  const match = dateStr.trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return new Date(Date.UTC(
    parseInt(match[1], 10),
    parseInt(match[2], 10) - 1,
    parseInt(match[3], 10),
    0, 0, 0
  ));
}

// Unified parser for ISO-like timestamp fragments:
// - All timestamp forms are treated as literal wall-clock values.
// - No timezone conversion is applied.
function parseTimestampFragment(fragment, options = {}) {
  if (typeof fragment !== 'string') return null;
  const clean = fragment.replace(/[']/g, '').trim();
  if (!clean) return null;

  if (isDateOnlyString(clean)) {
    return parseDateOnlyAsFloating(clean);
  }

  if (clean.includes('T')) {
    const c = extractLocalComponents(clean);
    if (c) {
      return new Date(Date.UTC(c.year, c.month, c.day, c.hours, c.minutes, c.seconds));
    }
  }

  const parsed = new Date(clean);
  if (isNaN(parsed.getTime())) return null;

  if (options.faceValue) {
    const c = extractLocalComponents(clean);
    if (c) {
      return new Date(Date.UTC(c.year, c.month, c.day, c.hours, c.minutes, c.seconds));
    }
  }

  return parsed;
}

function isOffsetTaggedRange(rangeStr) {
  if (typeof rangeStr !== 'string') return false;
  const cleanRange = rangeStr.replace(/[']/g, '').trim();
  if (!cleanRange.includes('/')) return false;
  const parts = cleanRange.split('/');
  if (parts.length !== 2) return false;
  return hasExplicitOffset(parts[0]) && hasExplicitOffset(parts[1]);
}

function isSameUTCDate(a, b) {
  return a.getUTCFullYear() === b.getUTCFullYear() &&
         a.getUTCMonth() === b.getUTCMonth() &&
         a.getUTCDate() === b.getUTCDate();
}

function toSecondsFromUTCClock(date) {
  return date.getUTCHours() * 3600 + date.getUTCMinutes() * 60 + date.getUTCSeconds();
}

function parseCalltimeSmart(calltimeStr) {
  if (!calltimeStr || typeof calltimeStr !== 'string') {
    return null;
  }

  const cleanStr = calltimeStr.replace(/[']/g, '').trim();
  if (!cleanStr) {
    return null;
  }

  // Parse calltime through the unified parser contract:
  // - all inputs are treated as literal wall-clock values
  const parsed = parseUnifiedDateTime(cleanStr);
  if (parsed?.start instanceof Date && !isNaN(parsed.start.getTime())) {
    return parsed;
  }

  return null;
}

function maybeCorrectMainEventEnd(eventDateRaw, eventTimes, parsedCalltime) {
  const result = { eventTimes, applied: false, reason: null };
  if (!eventTimes?.start || !eventTimes?.end || typeof eventDateRaw !== 'string') {
    return result;
  }

  const cleanRange = eventDateRaw.replace(/[']/g, '').trim();
  if (!cleanRange.includes('/') || !isOffsetTaggedRange(cleanRange)) {
    return result;
  }

  const [rawStartStr, rawEndStr] = cleanRange.split('/');
  const rawStart = extractLocalComponents(rawStartStr?.trim());
  const rawEnd = extractLocalComponents(rawEndStr?.trim());
  if (!rawEnd) {
    return result;
  }

  const anchorStart = parsedCalltime?.start instanceof Date && !isNaN(parsedCalltime.start.getTime())
    ? parsedCalltime.start
    : eventTimes.start;
  const parsedEnd = eventTimes.end;

  if (!(anchorStart instanceof Date) || isNaN(anchorStart.getTime())) {
    return result;
  }
  if (!(parsedEnd instanceof Date) || isNaN(parsedEnd.getTime())) {
    return result;
  }

  // Build a fallback end by placing the raw face-value hours on the anchor's date
  let fallbackEnd = new Date(Date.UTC(
    anchorStart.getUTCFullYear(),
    anchorStart.getUTCMonth(),
    anchorStart.getUTCDate(),
    rawEnd.hours,
    rawEnd.minutes,
    rawEnd.seconds
  ));
  if (fallbackEnd.getTime() <= anchorStart.getTime()) {
    fallbackEnd = new Date(fallbackEnd.getTime() + MAIN_EVENT_MS_PER_DAY);
  }

  const parsedDurationHours = (parsedEnd.getTime() - anchorStart.getTime()) / MAIN_EVENT_MS_PER_HOUR;
  const fallbackDurationHours = (fallbackEnd.getTime() - anchorStart.getTime()) / MAIN_EVENT_MS_PER_HOUR;

  const durationImplausible = parsedDurationHours <= 0 || parsedDurationHours > MAIN_EVENT_MAX_DURATION_HOURS;
  const rawEndEarlierThanAnchor = toSecondsFromUTCClock(anchorStart) > (rawEnd.hours * 3600 + rawEnd.minutes * 60 + rawEnd.seconds);
  const parsedEndSameDayAsAnchor = isSameUTCDate(parsedEnd, anchorStart);
  const overnightGuard = rawEndEarlierThanAnchor && parsedEndSameDayAsAnchor;
  const fallbackDurationValid = fallbackDurationHours > 0 && fallbackDurationHours <= MAIN_EVENT_MAX_DURATION_HOURS;

  const reasons = [];
  if (durationImplausible) reasons.push('implausible_duration');
  if (overnightGuard) reasons.push('overnight_guard');

  if (reasons.length === 0 || !fallbackDurationValid) {
    return result;
  }

  eventTimes.end = fallbackEnd;
  return { eventTimes, applied: true, reason: reasons.join('+') };
}

function applyCalltimeOverride(eventTimes, parsedCalltime) {
  if (!eventTimes?.start || !parsedCalltime?.start) {
    return eventTimes;
  }

  const startDate = eventTimes.start;
  const endDate = eventTimes.end;
  const calltimeDate = parsedCalltime.start;
  if (!(startDate instanceof Date) || isNaN(startDate.getTime())) {
    return eventTimes;
  }
  if (!(calltimeDate instanceof Date) || isNaN(calltimeDate.getTime())) {
    return eventTimes;
  }

  // Always anchor to event START date to preserve event-day semantics.
  let ctStart = new Date(Date.UTC(
    startDate.getUTCFullYear(),
    startDate.getUTCMonth(),
    startDate.getUTCDate(),
    calltimeDate.getUTCHours(),
    calltimeDate.getUTCMinutes(),
    calltimeDate.getUTCSeconds()
  ));

  // Guard malformed ranges: if calltime lands after end, use previous day only when needed.
  if (endDate instanceof Date && !isNaN(endDate.getTime()) && ctStart.getTime() > endDate.getTime()) {
    const previousDay = new Date(ctStart.getTime() - MAIN_EVENT_MS_PER_DAY);
    if (previousDay.getTime() <= endDate.getTime()) {
      ctStart = previousDay;
    }
  }

  eventTimes.start = ctStart;
  return eventTimes;
}

function resolveMainEventTimes(eventDateRaw, calltimeRaw, options = {}) {
  // Unified contract for event_date:
  // - all inputs are treated as literal wall-clock values
  // Keep legacy end-compat correction only for offset-tagged ranges with anomalies.
  const eventTimes = parseUnifiedDateTime(eventDateRaw, options);
  if (!eventTimes) {
    return {
      eventTimes: null,
      parsedCalltime: null,
      endCompatApplied: false,
      endCompatReason: null
    };
  }

  const parsedCalltime = parseCalltimeSmart(calltimeRaw);
  const endCompatResult = maybeCorrectMainEventEnd(eventDateRaw, eventTimes, parsedCalltime);
  const finalTimes = applyCalltimeOverride(endCompatResult.eventTimes, parsedCalltime);

  return {
    eventTimes: finalTimes,
    parsedCalltime,
    endCompatApplied: endCompatResult.applied,
    endCompatReason: endCompatResult.reason
  };
}

// Helper function to parse @ format dates (for flights, rehearsals, hotels, transport)
// Treats all timestamps as literal wall-clock values (no timezone conversion).
// options.faceValue: when true, extract literal time digits from ISO strings (used only for debugging)
function parseUnifiedDateTime(dateTimeStr, options = {}) {
  if (!dateTimeStr || dateTimeStr === null) {
    return null;
  }

  // Clean up the string
  const cleanStr = dateTimeStr.replace(/[']/g, '').trim();
  const atHumanNoConversion = options.atHumanNoConversion === true;

  // Global @ parser path: treat as literal wall clock for all feeds.
  if (cleanStr.startsWith('@')) {
    const strictParsed = parseAtHumanRangeNoConversion(cleanStr);
    if (strictParsed) {
      return strictParsed;
    }
    // If strict mode was requested, fail closed when @ input does not match.
    if (atHumanNoConversion) return null;
  }

  // Support human-readable ranges without '@', e.g.
  // "February 15, 2026 5:00 PM → 10:00 PM" or
  // "February 15, 2026 6:30 PM → February 16, 2026 12:00 AM"
  const humanRangeMatch = cleanStr.match(
    /^([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s*(?:→|->)\s*(?:([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+)?(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?$/i
  );
  if (humanRangeMatch) {
    const startDateStr = humanRangeMatch[1].trim();
    const startTimeStr = humanRangeMatch[2].trim();
    const explicitEndDateStr = humanRangeMatch[3] ? humanRangeMatch[3].trim() : null;
    const endTimeStr = humanRangeMatch[4].trim();
    const endDateStr = explicitEndDateStr || startDateStr;

    try {
      const startDate = parseHumanDateTimeAsFloating(startDateStr, startTimeStr);
      const endDate = parseHumanDateTimeAsFloating(endDateStr, endTimeStr);

      if (startDate && endDate && !isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
        let finalStart = startDate;
        let finalEnd = endDate;

        if (startDate.getTime() > endDate.getTime()) {
          finalEnd = new Date(endDate.getTime() + MAIN_EVENT_MS_PER_DAY);
        }

        return {
          start: finalStart,
          end: finalEnd
        };
      }
    } catch (e) {
      console.warn('Failed to parse non-@ human-readable date range:', cleanStr, e);
    }
  }
  
  // Check if it's the unified format with @
  if (cleanStr.startsWith('@')) {
    // First, try to match date-only format (for hotels): "@November 1, 2025 → November 2, 2025"
    const dateOnlyMatch = cleanStr.match(/@([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+→\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})/i);
    if (dateOnlyMatch) {
      try {
        const startDateStr = dateOnlyMatch[1].trim();
        const endDateStr = dateOnlyMatch[2].trim();

        const startDate = parseHumanDateAsFloating(startDateStr);
        const endDate = parseHumanDateAsFloating(endDateStr);
        
        if (startDate && endDate && !isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          return {
            start: startDate,
            end: endDate
          };
        }
      } catch (e) {
        console.warn('Failed to parse date-only format:', cleanStr, e);
      }
    }
    
    // Try to match format with times
    const match = cleanStr.match(/@(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))(?:\s+\([^)]+\))?\s+→\s+(.+)/i);
    if (match) {
      const dateStr = match[1].trim();
      const startTimeStr = match[2].trim();
      const endPart = match[3].trim();
      
      // Check if end part has a date (for multi-day events)
      let endTimeStr, endDateStr;
      const endMatch = endPart.match(/(.+?)\s+(\d{1,2}:\d{2}\s+(?:AM|PM))/i);
      if (endMatch && endMatch[1].toLowerCase().includes(',')) {
        // Multi-day format: "November 9, 2025 1:00 AM"
        endDateStr = endMatch[1].trim();
        endTimeStr = endMatch[2].trim();
      } else {
        // Same day format: "12:00 PM"
        endDateStr = dateStr;
        endTimeStr = endPart;
      }
      
      try {
        const startDate = parseHumanDateTimeAsFloating(dateStr, startTimeStr);
        const endDate = parseHumanDateTimeAsFloating(endDateStr, endTimeStr);
        
        if (startDate && endDate && !isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          // Keep start-date contract: don't swap start/end silently.
          // If end appears earlier than start, treat as overnight and roll end forward one day.
          let finalStart = startDate;
          let finalEnd = endDate;

          if (startDate.getTime() > endDate.getTime()) {
            finalEnd = new Date(endDate.getTime() + MAIN_EVENT_MS_PER_DAY);
          }

          return {
            start: finalStart,
            end: finalEnd
          };
        }
      } catch (e) {
        console.warn('Failed to parse unified date format:', cleanStr, e);
      }
    }
    
    // Fallback: try to parse as single date without end time
    const singleMatch = cleanStr.match(/@(.+)/);
    if (singleMatch) {
      try {
        const dateStr = singleMatch[1].trim();
        const humanDateTimeMatch = dateStr.match(/^([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+(\d{1,2}:\d{2}\s+(?:AM|PM))$/i);

        let date = null;
        if (humanDateTimeMatch) {
          date = parseHumanDateTimeAsFloating(humanDateTimeMatch[1].trim(), humanDateTimeMatch[2].trim());
        } else {
          date = parseTimestampFragment(dateStr, options);
        }
        
        if (date && !isNaN(date.getTime())) {
          return {
            start: date,
            end: date
          };
        }
      } catch (e) {
        console.warn('Failed to parse single date format:', cleanStr, e);
      }
    }
  }
  
  // Special handling for date range format: "2025-08-26T15:30:00+00:00/2025-09-14T06:00:00+00:00"
  if (cleanStr.includes('/')) {
    try {
      const parts = cleanStr.split('/');
      const firstStr = parts[0].trim();
      const secondStr = parts[1].trim();

      let actualStartDate = parseTimestampFragment(firstStr, options);
      let actualEndDate = parseTimestampFragment(secondStr, options);

      if (!actualStartDate || !actualEndDate || isNaN(actualStartDate.getTime()) || isNaN(actualEndDate.getTime())) {
        console.warn(`[parseUnifiedDateTime] Invalid range after normalization. Original: ${cleanStr}`);
        return null;
      }

      // Maintain source start date. If end is earlier, this is usually an overnight event.
      if (actualStartDate.getTime() > actualEndDate.getTime()) {
        const rolledEnd = new Date(actualEndDate.getTime() + MAIN_EVENT_MS_PER_DAY);
        actualEndDate = rolledEnd;
      }

      return {
        start: actualStartDate,
        end: actualEndDate
      };
    } catch (e) {
      console.warn('Failed to parse date range format:', cleanStr, e);
    }
  }
  
  // Fallback: try to parse as regular ISO date
  try {
    const normalized = parseTimestampFragment(cleanStr, options);
    if (normalized && !isNaN(normalized.getTime())) {
      return {
        start: normalized,
        end: normalized
      };
    }
  } catch (e) {
    console.warn('Failed to parse as ISO date:', cleanStr, e);
  }
  
  return null;
}

function getNestedRehearsals(event) {
  if (!event || typeof event !== 'object') return [];
  if (Array.isArray(event.rehearsals)) return event.rehearsals;
  if (Array.isArray(event.rehearsal)) return event.rehearsal;
  if (Array.isArray(event.Rehearsals)) return event.Rehearsals;
  return [];
}

function normalizeEventDateHelperString(raw) {
  if (typeof raw !== 'string') return '';
  return raw
    .replace(/\u00A0/g, ' ')
    .replace(/\u202F/g, ' ')
    .trim()
    .replace(/,+\s*$/, '');
}

function getHelperTimezoneOffsetHours(helperRaw) {
  if (typeof helperRaw !== 'string') return null;
  const tzMatch = helperRaw.match(/\((PST|PDT|HST|MST|MDT|CST|CDT|EST|EDT)\)/i);
  if (!tzMatch) return null;
  const zone = tzMatch[1].toUpperCase();
  const offsetMap = {
    PST: 8,
    PDT: 7,
    HST: 10,
    MST: 7,
    MDT: 6,
    CST: 6,
    CDT: 5,
    EST: 5,
    EDT: 4
  };
  return offsetMap[zone] ?? null;
}

function extractFirstHumanDateFromText(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const match = raw.match(/([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})/);
  if (!match) return null;
  const monthName = match[1];
  const day = Number(match[2]);
  const year = Number(match[3]);
  const monthIndex = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
  ].indexOf(monthName.toLowerCase());
  if (monthIndex < 0) return null;
  return Date.UTC(year, monthIndex, day);
}

function shiftDateByDays(value, deltaDays) {
  if (!deltaDays) return value instanceof Date ? new Date(value) : value;
  const date = value instanceof Date ? new Date(value) : new Date(value);
  if (isNaN(date.getTime())) return value;
  date.setUTCDate(date.getUTCDate() + deltaDays);
  return date;
}

function shiftRangeByDays(range, deltaDays) {
  if (!range || !deltaDays) return range;
  return {
    ...range,
    start: shiftDateByDays(range.start, deltaDays),
    end: shiftDateByDays(range.end, deltaDays)
  };
}

function alignEventTimesToDateHelper(event, baseEventTimes) {
  const helperRaw = normalizeEventDateHelperString(event?.event_date_helper);
  if (!helperRaw || !baseEventTimes?.start || !baseEventTimes?.end) {
    return {
      eventTimes: baseEventTimes,
      helperDeltaDays: 0,
      helperAdjusted: false,
      helperClockCorrected: false,
      helperClockCorrectionHours: 0
    };
  }

  // Keep parity with admin behavior: Event Date Helper anchors the DATE only.
  // Preserve times resolved from event_date/calltime and shift by whole-day delta.
  const helperDateUtc = extractFirstHumanDateFromText(helperRaw);
  if (helperDateUtc === null) {
    return {
      eventTimes: baseEventTimes,
      helperDeltaDays: 0,
      helperAdjusted: false,
      helperClockCorrected: false,
      helperClockCorrectionHours: 0
    };
  }

  const baseStartUtc = Date.UTC(
    baseEventTimes.start.getUTCFullYear(),
    baseEventTimes.start.getUTCMonth(),
    baseEventTimes.start.getUTCDate()
  );
  const helperDeltaDays = Math.round((helperDateUtc - baseStartUtc) / MAIN_EVENT_MS_PER_DAY);
  let alignedRange = helperDeltaDays ? shiftRangeByDays(baseEventTimes, helperDeltaDays) : baseEventTimes;
  let helperAdjusted = !!helperDeltaDays;
  let helperClockCorrected = false;
  let helperClockCorrectionHours = 0;

  // Some formula chains shift calltimes/starts by timezone offset while helper retains
  // the intended wall-clock event window. Correct only clear drift cases.
  const helperRange = parseUnifiedDateTime(helperRaw, { atHumanNoConversion: true });
  const helperOffsetHours = getHelperTimezoneOffsetHours(helperRaw);
  if (
    helperOffsetHours &&
    helperRange?.start instanceof Date &&
    !isNaN(helperRange.start.getTime()) &&
    alignedRange?.start instanceof Date &&
    !isNaN(alignedRange.start.getTime())
  ) {
    const correctedStart = new Date(alignedRange.start.getTime() - helperOffsetHours * MAIN_EVENT_MS_PER_HOUR);
    const leadHours = (helperRange.start.getTime() - correctedStart.getTime()) / MAIN_EVENT_MS_PER_HOUR;
    const currentStartsAfterHelper = alignedRange.start.getTime() > helperRange.start.getTime();

    // Apply when current start is later than helper start, and corrected start lands
    // in a realistic pre-show call window relative to helper start.
    if (currentStartsAfterHelper && leadHours >= 0 && leadHours <= 10) {
      alignedRange = {
        ...alignedRange,
        start: correctedStart,
        end: helperRange?.end instanceof Date && !isNaN(helperRange.end.getTime())
          ? new Date(helperRange.end)
          : alignedRange.end
      };
      helperClockCorrected = true;
      helperClockCorrectionHours = helperOffsetHours;
      helperAdjusted = true;
    }
  }

  return {
    eventTimes: alignedRange,
    helperDeltaDays,
    helperAdjusted,
    helperClockCorrected,
    helperClockCorrectionHours
  };
}

// Parse new structured format: "Driver: [driver_name:Name driver_phone:Phone,...]" -> [{name, phone}]
function parseStructuredDriverLine(line) {
  const results = [];
  if (!line || typeof line !== 'string') return results;
  const bracketMatch = line.match(/Driver:\s*\[([^\]]*)\]/);
  const content = bracketMatch ? bracketMatch[1] : '';
  const regex = /driver_name:(.+?)driver_phone:([^,\]]+)/g;
  let m;
  while ((m = regex.exec(content)) !== null) {
    results.push({ name: m[1].trim(), phone: m[2].trim() });
  }
  return results;
}

// Parse new structured format: "Passengers: [passenger_name:Name passenger_phone:Phone,...]" -> [{name, phone}]
function parseStructuredPassengerLine(line) {
  const results = [];
  if (!line || typeof line !== 'string') return results;
  const bracketMatch = line.match(/Passengers?:\s*\[([^\]]*)\]/i);
  const content = bracketMatch ? bracketMatch[1] : '';
  const regex = /passenger_name:(.+?)passenger_phone:([^,\]]+)/g;
  let m;
  while ((m = regex.exec(content)) !== null) {
    results.push({ name: m[1].trim(), phone: m[2].trim() });
  }
  return results;
}

// Parse "Driver Info: Name - (phone)" or "Name - (phone)" from description. Returns map of firstName -> phone.
function parseDriverInfoFromDescription(description) {
  const map = {};
  if (!description) return map;
  const driverInfoMatch = description.match(/Driver Info:\s*([\s\S]*?)(?=Group Text:|Confirmation:|Pick Up Info:|Drop Off Info:|Meet Up Info:|$)/i);
  const block = driverInfoMatch ? driverInfoMatch[1] : description;
  // Match "FirstName - (phone)" or "FirstName - phone" (phone can be various formats)
  const regex = /([A-Za-z]+)\s*-\s*(\([\d\s\-]+\)[\d\s\-]*|\+\d[\d\s\-]+|[\d\-]{10,})/g;
  let m;
  while ((m = regex.exec(block)) !== null) {
    map[m[1].toLowerCase()] = m[2].trim();
  }
  return map;
}

// Parse "Passenger Info:" or "Name - (phone)" for passengers. Returns map of name -> phone (or partial name match).
function parsePassengerInfoFromDescription(description) {
  const map = {};
  if (!description) return map;
  const passengerInfoMatch = description.match(/Passenger Info:\s*([\s\S]*?)(?=Driver Info:|Group Text:|Confirmation:|Pick Up Info:|Drop Off Info:|Meet Up Info:|$)/i);
  const block = passengerInfoMatch ? passengerInfoMatch[1] : '';
  const regex = /([A-Za-z][A-Za-z\s]+?)\s*-\s*(\([\d\s\-]+\)[\d\s\-]*|\+\d[\d\s\-]+|[\d\-]{10,})/g;
  let m;
  while ((m = regex.exec(block)) !== null) {
    const name = m[1].trim();
    const key = name.split(/\s+/)[0].toLowerCase();
    map[key] = { name, phone: m[2].trim() };
  }
  return map;
}

// Split comma-separated transport info (e.g. "Key: value, Key: value") into separate lines.
// Splits on comma when followed by a new key (Title Case + colon), preserving commas in addresses.
function formatTransportInfoLines(infoStr) {
  if (!infoStr || !infoStr.trim()) return [];
  const trimmed = infoStr.trim();
  // Split by newlines first
  const lines = trimmed.split(/\n/).filter(l => l.trim());
  const result = [];
  for (const line of lines) {
    // If line has comma-separated key-value pairs (Key: value, Key: value), split them
    // Split on comma when followed by a new key pattern: "Key: " (capital letter, then chars, then colon)
    const parts = line.split(/,\s*(?=[A-Z][A-Za-z\s#]+:)/);
    parts.forEach(part => {
      const p = part.trim();
      if (p) result.push(p);
    });
  }
  return result;
}

function formatTransportPeopleList(items, nameKey, phoneKey) {
  if (!Array.isArray(items)) return [];
  return items
    .map(item => {
      if (!item || typeof item !== 'object') return null;
      const name = typeof item[nameKey] === 'string' ? item[nameKey].trim() : '';
      const phone = typeof item[phoneKey] === 'string' ? item[phoneKey].trim() : '';
      if (!name) return null;
      return { name, phone };
    })
    .filter(Boolean);
}

function extractTransportInfoValue(rawDescription, transportType) {
  if (!rawDescription || typeof rawDescription !== 'object') return '';
  if (transportType === 'ground_transport_pickup') {
    return rawDescription['Pick Up Info'] || rawDescription['Pickup Info'] || '';
  }
  if (transportType === 'ground_transport_dropoff') {
    return rawDescription['Drop Off Info'] || rawDescription['Dropoff Info'] || '';
  }
  if (transportType === 'ground_transport_meeting') {
    return rawDescription['Meet Up Info'] || rawDescription['Meetup Info'] || '';
  }
  return (
    rawDescription['Pick Up Info'] ||
    rawDescription['Pickup Info'] ||
    rawDescription['Drop Off Info'] ||
    rawDescription['Dropoff Info'] ||
    rawDescription['Meet Up Info'] ||
    rawDescription['Meetup Info'] ||
    ''
  );
}

function buildTransportDescription(transport) {
  const rawDescription = transport?.description;
  if (!rawDescription) {
    return 'Ground transportation details';
  }

  if (typeof rawDescription === 'string') {
    let description = '';
    const driverMatch = rawDescription.match(/Driver:\s*([^\n]+)/);
    const passengerMatch = rawDescription.match(/Passengers?:\s*([^\n]+)/);
    const driverContent = driverMatch ? driverMatch[1].trim() : '';
    const passengerContent = passengerMatch ? passengerMatch[1].trim() : '';
    const isStructuredDriver = driverContent.startsWith('[');
    const isStructuredPassenger = passengerContent.startsWith('[');

    if (driverMatch) {
      if (isStructuredDriver) {
        const drivers = parseStructuredDriverLine('Driver: ' + driverContent);
        if (drivers.length > 0) {
          description += 'Drivers:\n';
          drivers.forEach(({ name, phone }) => {
            description += `- ${name}${phone ? ` ${phone}` : ''}\n`;
          });
          description += '\n';
        }
      } else {
        const driverPhones = parseDriverInfoFromDescription(rawDescription);
        const drivers = driverContent.split(',').map(d => d.trim()).filter(d => d);
        if (drivers.length > 0) {
          description += 'Drivers:\n';
          drivers.forEach(driver => {
            const firstName = driver.split(/\s+/)[0]?.toLowerCase() || '';
            const phone = driverPhones[firstName];
            description += `- ${driver}${phone ? ` ${phone}` : ''}\n`;
          });
          description += '\n';
        }
      }
    }

    if (passengerMatch) {
      if (isStructuredPassenger) {
        const passengers = parseStructuredPassengerLine('Passengers: ' + passengerContent);
        if (passengers.length > 0) {
          description += 'Passengers:\n';
          passengers.forEach(({ name, phone }) => {
            description += `- ${name}${phone ? ` ${phone}` : ''}\n`;
          });
          description += '\n';
        }
      } else {
        const passengerPhones = parsePassengerInfoFromDescription(rawDescription);
        const passengers = passengerContent.split(',').map(p => p.trim()).filter(p => p && p !== 'TBD');
        if (passengers.length > 0) {
          description += 'Passengers:\n';
          passengers.forEach(passenger => {
            const firstName = passenger.split(/\s+/)[0]?.toLowerCase() || '';
            const phone = passengerPhones[firstName]?.phone;
            description += `- ${passenger}${phone ? ` ${phone}` : ''}\n`;
          });
          description += '\n';
        }
      }
    }

    if (transport.type === 'ground_transport_pickup') {
      const pickupInfoMatch = rawDescription.match(/Pick Up Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
      if (pickupInfoMatch) {
        const pickupInfo = pickupInfoMatch[1].trim();
        if (pickupInfo) {
          description += 'Pick Up Info:\n';
          formatTransportInfoLines(pickupInfo).forEach(line => {
            description += `• ${line}\n`;
          });
          description += '\n';
        }
      }
    } else if (transport.type === 'ground_transport_dropoff') {
      const dropoffInfoMatch = rawDescription.match(/Drop Off Info:\s*([\s\S]*?)(?=Confirmation:|$)/);
      if (dropoffInfoMatch) {
        const dropoffInfo = dropoffInfoMatch[1].trim();
        if (dropoffInfo) {
          description += 'Drop Off Info:\n';
          formatTransportInfoLines(dropoffInfo).forEach(line => {
            description += `• ${line}\n`;
          });
          description += '\n';
        }
      }
    }

    return description.trim() || 'Ground transportation details';
  }

  if (typeof rawDescription === 'object') {
    let description = '';
    const drivers = formatTransportPeopleList(rawDescription.Driver || rawDescription.Drivers, 'driver_name', 'driver_phone');
    const passengers = formatTransportPeopleList(rawDescription.Passengers || rawDescription.Passenger, 'passenger_name', 'passenger_phone');
    const infoValue = extractTransportInfoValue(rawDescription, transport.type);
    const confirmation = typeof rawDescription.Confirmation === 'string'
      ? rawDescription.Confirmation.trim()
      : (typeof rawDescription.confirmation === 'string' ? rawDescription.confirmation.trim() : '');

    if (drivers.length > 0) {
      description += 'Drivers:\n';
      drivers.forEach(({ name, phone }) => {
        description += `- ${name}${phone ? ` ${phone}` : ''}\n`;
      });
      description += '\n';
    }

    if (passengers.length > 0) {
      description += 'Passengers:\n';
      passengers.forEach(({ name, phone }) => {
        description += `- ${name}${phone ? ` ${phone}` : ''}\n`;
      });
      description += '\n';
    }

    if (infoValue) {
      const heading = transport.type === 'ground_transport_pickup'
        ? 'Pick Up Info'
        : transport.type === 'ground_transport_dropoff'
          ? 'Drop Off Info'
          : 'Transport Info';
      description += `${heading}:\n`;
      formatTransportInfoLines(String(infoValue)).forEach(line => {
        description += `• ${line}\n`;
      });
      description += '\n';
    }

    if (confirmation) {
      description += `Confirmation: ${confirmation}\n`;
    }

    return description.trim() || 'Ground transportation details';
  }

  return 'Ground transportation details';
}

function getTransportEventTimes(transport) {
  const startParsed = parseUnifiedDateTime(transport?.start);
  if (!startParsed) return null;

  const startTime = new Date(startParsed.start);
  let endTime = new Date(startParsed.end);

  if (transport?.end) {
    const endParsed = parseUnifiedDateTime(transport.end);
    if (endParsed) {
      endTime = new Date(endParsed.end);
    }
  }

  if (isNaN(endTime.getTime()) || endTime.getTime() < startTime.getTime()) {
    endTime = new Date(startTime.getTime() + 30 * 60 * 1000);
  }

  return { startTime, endTime };
}

// Same logic as travel calendar: parse departure and arrival separately when both exist.
// Use this for personal-calendar flight events so times match the travel calendar.
function getFlightLegTimes(departureTimeStr, arrivalTimeStr) {
  if (departureTimeStr && arrivalTimeStr) {
    const startParsed = parseUnifiedDateTime(departureTimeStr);
    const endParsed = parseUnifiedDateTime(arrivalTimeStr);
    if (startParsed && endParsed && !isNaN(startParsed.start.getTime()) && !isNaN(endParsed.start.getTime())) {
      return { start: startParsed.start, end: endParsed.end };
    }
  }
  const parsed = parseUnifiedDateTime(departureTimeStr);
  if (parsed) return parsed;
  if (departureTimeStr) {
    return { start: departureTimeStr, end: arrivalTimeStr || departureTimeStr };
  }
  return null;
}

// Helper function to rebuild and cache a single person's calendar.
async function regenerateCalendarForPerson(personId, options = {}) {
  const { trigger = 'unknown', clearCache = false, preloadedCalendarData = null, regenMode = REGEN_MODE_FULL } = options;
  const regenStartedAt = Date.now();
  const selectedRegenMode = parseRegenMode(regenMode) || REGEN_MODE_FULL;
  const splitMode = selectedRegenMode !== REGEN_MODE_FULL;
  const splitModeAllowed = isSplitModeAllowedForPerson(personId, selectedRegenMode);
  const cachePrefix = selectedRegenMode === REGEN_MODE_FULL
    ? `calendar:${personId}`
    : `calendar:${personId}:${selectedRegenMode}`;

  try {
    if (isNotionCircuitOpen()) {
      const waitMs = Math.max(0, notionCircuitOpenUntil - Date.now());
      const reason = `Notion circuit open (${waitMs}ms remaining)`;
      return { success: false, personId, error: reason };
    }
    if (splitMode && !splitModeAllowed) {
      return {
        success: false,
        personId,
        reason: 'mode_not_allowed',
        error: `regen mode "${selectedRegenMode}" is only allowed for test person ${SPLIT_REGEN_TEST_PERSON_ID}`
      };
    }
    console.log(`🔄 Rebuilding calendar for ${personId}...`);
    if (splitMode) {
      console.log(`🧪 Split regen mode "${selectedRegenMode}" enabled for ${personId}`);
    }
    if (clearCache && redis && cacheEnabled) {
      await redis.del(`${cachePrefix}:ics`);
      await redis.del(`${cachePrefix}:google_ics`);
      await redis.del(`${cachePrefix}:json`);
    }

    // Enforce a hard rebuild time budget to prevent indefinite requests.
    let calendarData = preloadedCalendarData;
    if (!calendarData) {
      const deadlineTs = Date.now() + REGEN_TOTAL_TIMEOUT_MS;
      const fetchTimeoutMs = Math.min(REGEN_FETCH_STEP_TIMEOUT_MS, getRemainingMs(deadlineTs));
      calendarData = await withTimeout(
        selectedRegenMode === REGEN_MODE_EVENTS_ONLY
          ? getCalendarDataEventsOnlyFromPersonRelationPath(personId, 6)
          : selectedRegenMode === REGEN_MODE_NON_EVENTS_ONLY
            ? getCalendarDataNonEventsOnlyFromDatabaseQueryStyle(personId, 6)
            : getCalendarDataFromDatabaseQueryStyle(personId, 6),
        fetchTimeoutMs,
        'Personnel calendar-data fetch timed out'
      );
    }

    if (selectedRegenMode === REGEN_MODE_EVENTS_ONLY && calendarData) {
      calendarData = coerceCalendarDataToEventsOnly(calendarData);
    } else if (selectedRegenMode === REGEN_MODE_NON_EVENTS_ONLY && calendarData) {
      calendarData = coerceCalendarDataToNonEventsOnly(calendarData);
    }

    if (!calendarData) {
      console.log(`⚠️  No calendar data found for ${personId}, skipping...`);
      return { success: false, personId, reason: 'no_events' };
    }
    
    const events = calendarData;
    const eventsArray = Array.isArray(events) ? events : events.events || [];
    const topLevelFlights = events.flights || [];
    const topLevelRehearsals = events.rehearsals || [];
    const topLevelHotels = events.hotels || [];
    const topLevelTransport = events.ground_transport || [];
    const topLevelTeamCalendar = events.team_calendar || [];
    const topLevelEventNoteReminders = events.event_note_reminders || [];
    const hasAnySourceData =
      eventsArray.length > 0 ||
      topLevelFlights.length > 0 ||
      topLevelRehearsals.length > 0 ||
      topLevelHotels.length > 0 ||
      topLevelTransport.length > 0 ||
      topLevelTeamCalendar.length > 0 ||
      topLevelEventNoteReminders.length > 0;

    if (!hasAnySourceData) {
      console.warn(`⚠️  No source event data for ${personId}: events=${eventsArray.length}, flights=${topLevelFlights.length}, rehearsals=${topLevelRehearsals.length}, hotels=${topLevelHotels.length}, transport=${topLevelTransport.length}, team=${topLevelTeamCalendar.length}, reminders=${topLevelEventNoteReminders.length}`);
      return { success: false, personId, reason: 'no_events' };
    }
    
    const personName = calendarData.personName || 'Unknown';
    const firstName = personName.split(' ')[0];
    
    console.log(`Processing calendar for ${personName} (${calendarData.events.length} events, mode=${selectedRegenMode})`);

    // Process events into calendar format (duplicated from main endpoint)
    const allCalendarEvents = [];
    
    eventsArray.forEach(event => {
      let helperDeltaDays = 0;
      // Add main event
      if (event.event_name && event.event_date) {
        const mainEventTimeResult = resolveMainEventTimes(event.event_date, event.calltime);
        const alignmentResult = alignEventTimesToDateHelper(event, mainEventTimeResult.eventTimes);
        const eventTimes = alignmentResult.eventTimes;
        helperDeltaDays = alignmentResult.helperDeltaDays || 0;
        
        if (eventTimes) {
          let payrollInfo = '';
          const positionValue = typeof event.position === 'string' ? event.position.trim() : event.position;
          const assignmentsValue = typeof event.assignments === 'string' ? event.assignments.trim() : event.assignments;
          const payTotalRaw = event.pay_total;
          const payTotalStr = payTotalRaw === 0 ? '0' : (payTotalRaw ?? '').toString().trim();
          const hasPosition = positionValue !== undefined && positionValue !== null && `${positionValue}`.trim() !== '';
          const hasAssignments = assignmentsValue !== undefined && assignmentsValue !== null && `${assignmentsValue}`.trim() !== '';
          const hasPayTotal = payTotalRaw !== null && payTotalRaw !== undefined && payTotalStr !== '';

          if (hasPosition || hasAssignments || hasPayTotal) {
            if (hasPosition) payrollInfo += `Position: ${positionValue}\n`;
            if (hasAssignments) payrollInfo += `Assignments: ${assignmentsValue}\n`;
            if (hasPayTotal) {
              const payDisplay = payTotalStr.startsWith('$') ? payTotalStr : `$${payTotalStr}`;
              payrollInfo += `Pay: ${payDisplay}\n`;
            }
            payrollInfo += '\n';
          }

          let calltimeInfo = '';
          if (event.calltime) {
            const displayCalltime = alignmentResult.helperClockCorrected
              ? formatFloatingTimeFromDate(eventTimes.start)
              : formatCallTime(event.calltime);
            calltimeInfo = `➡️ Call Time: ${displayCalltime}\n\n`;
          }

          let gearChecklistInfo = '';
          if (event.gear_checklist && event.gear_checklist.trim()) {
            gearChecklistInfo = `🔧 Gear Checklist: ${event.gear_checklist}\n\n`;
          }

          let eventPersonnelInfo = '';
          if (event.event_personnel && event.event_personnel.trim()) {
            eventPersonnelInfo = `👥 Event Personnel:\n${event.event_personnel}\n\n`;
          }

          let notionUrlInfo = '';
          if (event.notion_url && event.notion_url.trim()) {
            notionUrlInfo = `Notion Link: ${event.notion_url}\n\n`;
          }

          allCalendarEvents.push({
            type: 'main_event',
            title: `🎸 ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
            start: eventTimes.start,
            end: eventTimes.end,
            description: payrollInfo + calltimeInfo + gearChecklistInfo + eventPersonnelInfo + notionUrlInfo + (event.general_info || ''),
            location: event.venue_address || event.venue || '',
            band: event.band || '',
            mainEvent: event.event_name
          });
        }
      }
      
      // Add flight events (same logic as travel calendar: parse start/arrival separately when both exist)
      if (event.flights && Array.isArray(event.flights)) {
        event.flights.forEach(flight => {
          if (flight.departure_time && flight.departure_name) {
            let departureTimes = getFlightLegTimes(flight.departure_time, flight.departure_arrival_time);
            if (!departureTimes) {
              departureTimes = {
                start: flight.departure_time,
                end: flight.departure_arrival_time || flight.departure_time
              };
            }
            departureTimes = shiftRangeByDays(departureTimes, helperDeltaDays);
            
            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          if (flight.return_time && flight.return_name) {
            let returnTimes = getFlightLegTimes(flight.return_time, flight.return_arrival_time);
            if (!returnTimes) {
              returnTimes = {
                start: flight.return_time,
                end: flight.return_arrival_time || flight.return_time
              };
            }
            returnTimes = shiftRangeByDays(returnTimes, helperDeltaDays);
            
            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Departure layover flight
          if (flight.departure_lo_time && flight.departure_lo_flightnumber) {
            let departureLoTimes = parseUnifiedDateTime(flight.departure_lo_time);
            if (!departureLoTimes) {
              departureLoTimes = {
                start: flight.departure_lo_time,
                end: flight.departure_lo_time
              };
            }
            departureLoTimes = shiftRangeByDays(departureLoTimes, helperDeltaDays);

            allCalendarEvents.push({
              type: 'flight_departure_layover',
              title: `✈️ Layover: ${flight.departure_lo_from_airport || 'N/A'} → ${flight.departure_lo_to_airport || 'N/A'}`,
              start: departureLoTimes.start,
              end: departureLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_lo_flightnumber || 'N/A'}\nFrom: ${flight.departure_lo_from_airport || 'N/A'}\nTo: ${flight.departure_lo_to_airport || 'N/A'}`,
              location: flight.departure_lo_from_airport_address || flight.departure_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }

          // Return layover flight
          if (flight.return_lo_time && flight.return_lo_flightnumber) {
            let returnLoTimes = parseUnifiedDateTime(flight.return_lo_time);
            if (!returnLoTimes) {
              returnLoTimes = {
                start: flight.return_lo_time,
                end: flight.return_lo_time
              };
            }
            returnLoTimes = shiftRangeByDays(returnLoTimes, helperDeltaDays);

            allCalendarEvents.push({
              type: 'flight_return_layover',
              title: `✈️ Layover: ${flight.return_lo_from_airport || 'N/A'} → ${flight.return_lo_to_airport || 'N/A'}`,
              start: returnLoTimes.start,
              end: returnLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_lo_flightnumber || 'N/A'}\nFrom: ${flight.return_lo_from_airport || 'N/A'}\nTo: ${flight.return_lo_to_airport || 'N/A'}`,
              location: flight.return_lo_from_airport_address || flight.return_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add rehearsal events (same logic)
      const nestedRehearsals = getNestedRehearsals(event);
      if (nestedRehearsals.length > 0) {
        nestedRehearsals.forEach(rehearsal => {
          if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
            let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
            rehearsalTimes = shiftRangeByDays(rehearsalTimes, helperDeltaDays);
            let location = 'TBD';
            if (rehearsal.rehearsal_location && rehearsal.rehearsal_address) {
              location = `${rehearsal.rehearsal_location}, ${rehearsal.rehearsal_address}`;
            } else if (rehearsal.rehearsal_location) {
              location = rehearsal.rehearsal_location;
            } else if (rehearsal.rehearsal_address) {
              location = rehearsal.rehearsal_address;
            }

            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal - ${event.event_name}${event.band ? ` (${event.band})` : ''}`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add hotel events (same logic)
      if (event.hotels && Array.isArray(event.hotels)) {
        event.hotels.forEach(hotel => {
          let hotelTimes = null;
          
          if (hotel.dates_booked) {
            hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
          } else if (hotel.check_in && hotel.check_out) {
            try {
              const startParsed = parseUnifiedDateTime(hotel.check_in);
              const endParsed = parseUnifiedDateTime(hotel.check_out);
              if (startParsed && endParsed) {
                hotelTimes = { start: startParsed.start, end: endParsed.end };
              }
            } catch (e) {
              console.warn('Unable to parse hotel dates:', hotel.check_in, hotel.check_out);
              return;
            }
          }
          hotelTimes = shiftRangeByDays(hotelTimes, helperDeltaDays);

          if (hotelTimes) {
            let namesFormatted = 'N/A';
            if (hotel.names_on_reservation) {
              const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
              if (names.length > 0) {
                namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
              }
            }

            allCalendarEvents.push({
              type: 'hotel',
              title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
              start: hotelTimes.start,
              end: hotelTimes.end,
              description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
              location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
              url: hotel.hotel_url || '',
              confirmation: hotel.confirmation || '',
              hotelName: hotel.hotel_name || '',
              mainEvent: event.event_name
            });
          }
        });
      }

      // Add ground transport events (same logic)
      if (event.ground_transport && Array.isArray(event.ground_transport)) {
        event.ground_transport.forEach(transport => {
          if (transport.start) {
            const startParsed = shiftRangeByDays(parseUnifiedDateTime(transport.start), helperDeltaDays);
            const endParsed = transport.end ? shiftRangeByDays(parseUnifiedDateTime(transport.end), helperDeltaDays) : null;
            if (!startParsed) {
              return;
            }

            const startTime = new Date(startParsed.start);
            const endTime = endParsed?.end instanceof Date && !isNaN(endParsed.end.getTime())
              ? new Date(endParsed.end)
              : new Date(startTime.getTime() + 30 * 60 * 1000);

            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');
            const description = buildTransportDescription(transport);

            allCalendarEvents.push({
              type: transport.type || 'ground_transport',
              title: `🚙 ${formattedTitle}`,
              start: startTime,
              end: endTime,
              description,
              location: transport.location || '',
              url: transport.transportation_url || '',
              mainEvent: event.event_name
            });
          }
        });
      }
    });
    
    // Process top-level arrays (same logic as main endpoint)
    if (topLevelFlights.length > 0) {
      topLevelFlights.forEach(flight => {
        if (flight.departure_time && flight.departure_name) {
          let departureTimes = getFlightLegTimes(flight.departure_time, flight.departure_arrival_time);
          if (departureTimes) {
            let description = `Airline: ${flight.departure_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight departure
            const departureTimeStart = departureTimes.start instanceof Date ? departureTimes.start.toISOString() : new Date(departureTimes.start).toISOString();
            const departureTimeEnd = departureTimes.end instanceof Date ? departureTimes.end.toISOString() : new Date(departureTimes.end).toISOString();
            const departureTimeRange = `${departureTimeStart}/${departureTimeEnd}`;
            const route = `${flight.departure_airport || 'N/A'}-${flight.return_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.departure_flightnumber || 'N/A',
              departureTime: departureTimeRange,
              airline: flight.departure_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.departure_airport || 'N/A',
              arrivalCode: flight.return_airport || 'N/A',
              departureName: flight.departure_airport_name || 'N/A',
              arrivalName: flight.return_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'departure');

            allCalendarEvents.push({
              type: 'flight_departure',
              title: `✈️ ${flight.departure_name || 'Flight Departure'}`,
              start: departureTimes.start,
              end: departureTimes.end,
              description: description,
              location: flight.departure_airport_address || flight.departure_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }
        }

        if (flight.return_time && flight.return_name) {
          let returnTimes = getFlightLegTimes(flight.return_time, flight.return_arrival_time);
          if (returnTimes) {
            let description = `Airline: ${flight.return_airline || 'N/A'}\nConfirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_flightnumber || 'N/A'} <-- hold for tracking`;
            
            // Generate countdown URL for flight return
            const returnTimeStart = returnTimes.start instanceof Date ? returnTimes.start.toISOString() : new Date(returnTimes.start).toISOString();
            const returnTimeEnd = returnTimes.end instanceof Date ? returnTimes.end.toISOString() : new Date(returnTimes.end).toISOString();
            const returnTimeRange = `${returnTimeStart}/${returnTimeEnd}`;
            const route = `${flight.return_airport || 'N/A'}-${flight.departure_airport || 'N/A'}`;
            const countdownUrl = generateFlightCountdownUrl({
              flightNumber: flight.return_flightnumber || 'N/A',
              departureTime: returnTimeRange,
              airline: flight.return_airline || 'N/A',
              route: route,
              confirmation: flight.confirmation || 'N/A',
              departureCode: flight.return_airport || 'N/A',
              arrivalCode: flight.departure_airport || 'N/A',
              departureName: flight.return_airport_name || 'N/A',
              arrivalName: flight.departure_airport_name || 'N/A',
              flight_url: flight.flight_url
            }, 'return');

            allCalendarEvents.push({
              type: 'flight_return',
              title: `✈️ ${flight.return_name || 'Flight Return'}`,
              start: returnTimes.start,
              end: returnTimes.end,
              description: description,
              location: flight.return_airport_address || flight.return_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }

          // Departure layover flight
          if (flight.departure_lo_time && flight.departure_lo_flightnumber) {
            let departureLoTimes = parseUnifiedDateTime(flight.departure_lo_time);
            if (!departureLoTimes) {
              departureLoTimes = {
                start: flight.departure_lo_time,
                end: flight.departure_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_departure_layover',
              title: `✈️ Layover: ${flight.departure_lo_from_airport || 'N/A'} → ${flight.departure_lo_to_airport || 'N/A'}`,
              start: departureLoTimes.start,
              end: departureLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.departure_lo_flightnumber || 'N/A'}\nFrom: ${flight.departure_lo_from_airport || 'N/A'}\nTo: ${flight.departure_lo_to_airport || 'N/A'}`,
              location: flight.departure_lo_from_airport_address || flight.departure_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.departure_airline || '',
              flightNumber: flight.departure_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }

          // Return layover flight
          if (flight.return_lo_time && flight.return_lo_flightnumber) {
            let returnLoTimes = parseUnifiedDateTime(flight.return_lo_time);
            if (!returnLoTimes) {
              returnLoTimes = {
                start: flight.return_lo_time,
                end: flight.return_lo_time
              };
            }

            allCalendarEvents.push({
              type: 'flight_return_layover',
              title: `✈️ Layover: ${flight.return_lo_from_airport || 'N/A'} → ${flight.return_lo_to_airport || 'N/A'}`,
              start: returnLoTimes.start,
              end: returnLoTimes.end,
              description: `Confirmation: ${flight.confirmation || 'N/A'}\nFlight #: ${flight.return_lo_flightnumber || 'N/A'}\nFrom: ${flight.return_lo_from_airport || 'N/A'}\nTo: ${flight.return_lo_to_airport || 'N/A'}`,
              location: flight.return_lo_from_airport_address || flight.return_lo_from_airport || '',
              url: flight.flight_url || '',
              airline: flight.return_airline || '',
              flightNumber: flight.return_lo_flightnumber || '',
              confirmation: flight.confirmation || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelRehearsals.length > 0) {
      topLevelRehearsals.forEach(rehearsal => {
        if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
          let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
          
          if (rehearsalTimes) {
            let location = rehearsal.rehearsal_address ? rehearsal.rehearsal_address.trim().replace(/\u2060/g, '') : 'TBD';
            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_pay) {
              description += `\n\nRehearsal Pay - $${rehearsal.rehearsal_pay}`;
            }
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: `🎤 Rehearsal`,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_notion_url || rehearsal.rehearsal_pco || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelHotels.length > 0) {
      topLevelHotels.forEach(hotel => {
        let hotelTimes = null;
        if (hotel.dates_booked) {
          hotelTimes = parseUnifiedDateTime(hotel.dates_booked);
        }

        if (hotelTimes) {
          let namesFormatted = 'N/A';
          if (hotel.names_on_reservation) {
            const names = hotel.names_on_reservation.split(',').map(n => n.trim()).filter(n => n);
            if (names.length > 0) {
              namesFormatted = '\n' + names.map(name => `${name}`).join('\n');
            }
          }

          allCalendarEvents.push({
            type: 'hotel',
            title: `🏨 ${hotel.hotel_name || hotel.title || 'Hotel'}`,
            start: hotelTimes.start,
            end: hotelTimes.end,
            description: `Hotel Stay\nConfirmation: ${hotel.confirmation || 'N/A'}\nPhone: ${hotel.hotel_phone || 'N/A'}\n\nNames on Reservation:${namesFormatted}\nBooked Under: ${hotel.booked_under || 'N/A'}${hotel.hotel_url ? '\n\nNotion Link: ' + hotel.hotel_url : ''}`,
            location: hotel.hotel_address || hotel.hotel_name || 'Hotel',
            url: hotel.hotel_url || '',
            confirmation: hotel.confirmation || '',
            hotelName: hotel.hotel_name || '',
            mainEvent: ''
          });
        }
      });
    }

    if (topLevelTransport.length > 0) {
      topLevelTransport.forEach(transport => {
        if (transport.start) {
          const transportEventTimes = getTransportEventTimes(transport);
          if (transportEventTimes) {
            const { startTime, endTime } = transportEventTimes;

            let formattedTitle = transport.title || 'Ground Transport';
            formattedTitle = formattedTitle.replace('PICKUP:', 'Pickup:').replace('DROPOFF:', 'Dropoff:').replace('MEET UP:', 'Meet Up:');
            const description = buildTransportDescription(transport);
            
            let eventType = 'ground_transport';
            if (transport.type === 'ground_transport_pickup') {
              eventType = 'ground_transport_pickup';
            } else if (transport.type === 'ground_transport_dropoff') {
              eventType = 'ground_transport_dropoff';
            } else if (transport.type === 'ground_transport_meeting') {
              eventType = 'ground_transport_meeting';
            }

            allCalendarEvents.push({
              type: eventType,
              title: `🚙 ${formattedTitle}`,
              start: startTime,
              end: endTime,
              description: description,
              location: transport.location || '',
              url: transport.transportation_url || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelTeamCalendar.length > 0) {
      topLevelTeamCalendar.forEach(teamEvent => {
        if (teamEvent.date) {
          let eventTimes = parseUnifiedDateTime(teamEvent.date);
          if (eventTimes) {
            const isOOO = teamEvent.title && teamEvent.title.trim().toUpperCase() === 'OOO';
            const isMeeting = teamEvent.title && teamEvent.title.trim().toUpperCase().includes('MEETING');
            let emoji;
            if (isOOO) {
              emoji = '⛔️';
            } else if (isMeeting) {
              emoji = '💼';
            } else {
              emoji = '📅';
            }
            
            // For OOO events, add one day to end date to make it inclusive
            // In iCal format, end date is exclusive, so we need Dec 17 to block through Dec 16
            let endDate = eventTimes.end;
            if (isOOO) {
              endDate = new Date(eventTimes.end);
              endDate.setDate(endDate.getDate() + 1);
            }
            
            allCalendarEvents.push({
              type: 'team_calendar',
              title: `${emoji} ${teamEvent.title || 'Team Event'}`,
              start: eventTimes.start,
              end: endDate,
              description: [teamEvent.dcos, teamEvent.notes].filter(Boolean).join('\n\n'),
              location: teamEvent.address || '',
              url: teamEvent.notion_link || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    if (topLevelEventNoteReminders.length > 0) {
      topLevelEventNoteReminders.forEach(reminder => {
        if (reminder.remind_date) {
          const reminderTimes = parseUnifiedDateTime(reminder.remind_date);
          if (reminderTimes) {
            const reminderDescription = [reminder.event_name, reminder.description].filter(Boolean).join('\n\n');
            allCalendarEvents.push({
              type: 'event_note_reminder',
              title: '🔔 Event Reminder',
              start: reminderTimes.start,
              end: reminderTimes.end,
              description: reminderDescription,
              location: '',
              url: reminder.notion_link || '',
              mainEvent: ''
            });
          }
        }
      });
    }

    // Generate ICS calendar
    const calendar = ical({ 
      name: `Downbeat iCal (${firstName})`,
      description: `Professional events calendar for ${personName}`,
      ttl: 300  // Suggest refresh every 5 minutes
    });

    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });

    const icsData = serializeCalendar(calendar);
    const googleIcsData = serializeGoogleCalendar(calendar);

    const jsonResponse = {
      personName,
      regenMode: selectedRegenMode,
      totalMainEvents: eventsArray.length,
      totalCalendarEvents: allCalendarEvents.length,
      dataSource: 'calendar_data_database',
      breakdown: {
        mainEvents: allCalendarEvents.filter(e => e.type === 'main_event').length,
        flights: allCalendarEvents.filter(e => e.type === 'flight_departure' || e.type === 'flight_return' || e.type === 'flight_departure_layover' || e.type === 'flight_return_layover').length,
        rehearsals: allCalendarEvents.filter(e => e.type === 'rehearsal').length,
        hotels: allCalendarEvents.filter(e => e.type === 'hotel').length,
        groundTransport: allCalendarEvents.filter(e => e.type === 'ground_transport_pickup' || e.type === 'ground_transport_dropoff' || e.type === 'ground_transport_meeting' || e.type === 'ground_transport').length,
        teamCalendar: allCalendarEvents.filter(e => e.type === 'team_calendar').length,
        eventReminders: allCalendarEvents.filter(e => e.type === 'event_note_reminder').length
      },
      events: allCalendarEvents
    };
    const jsonData = JSON.stringify(jsonResponse);

    // Cache both formats.
    if (redis && cacheEnabled) {
      await setCalendarCache(`${cachePrefix}:ics`, icsData);
      await setCalendarCache(`${cachePrefix}:google_ics`, googleIcsData);
      await setCalendarCache(`${cachePrefix}:json`, jsonData);
      verboseLog(`✅ Cached calendar for ${personName} (${allCalendarEvents.length} events, expires in ${CACHE_TTL}s)`);
    }

    return {
      success: true,
      personId,
      personName,
      regenMode: selectedRegenMode,
      eventCount: allCalendarEvents.length,
      icsData,
      googleIcsData,
      jsonData,
      jsonResponse,
      allCalendarEvents
    };
    
  } catch (error) {
    const elapsedMs = Date.now() - regenStartedAt;
    console.error(`❌ Error regenerating calendar for ${personId}:`, error.message);
    console.error(`❌ Regen failure details for ${personId}: trigger=${trigger}, elapsedMs=${elapsedMs}`);
    // #region agent log
    fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'personnel_regen_compare',hypothesisId:'H2',location:'index.js:regenerateCalendarForPerson:catch',message:'Personnel regeneration failed',data:{personId,trigger,error:error?.message||'unknown'},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    return { success: false, personId, error: error.message };
  }
}

// Helper function to regenerate all calendars using batched parallel processing
async function regenerateAllCalendars() {
  const startTime = Date.now();
  
  try {
    if (isNotionCircuitOpen()) {
      const waitMs = Math.max(0, notionCircuitOpenUntil - Date.now());
      return { success: false, error: `Notion circuit open (${waitMs}ms remaining)`, timeSeconds: 0 };
    }
    console.log('🚀 Starting bounded-concurrency calendar regeneration...');
    const concurrency = Math.max(1, DEFAULT_REGEN_CONCURRENCY);
    console.log(`Processing Calendar Data with worker concurrency=${concurrency} and pageSize=${CALENDAR_DATA_SWEEP_PAGE_SIZE}`);

    const { results: allResults, totalRows, pageCount } = await processCalendarDataRowsPaginated({
      trigger: 'bulk_regen',
      concurrency,
      maxRetries: 5
    });

    const totalSuccess = allResults.filter(r => r.success).length;
    const totalSkipped = allResults.filter(r => r.reason === 'no_events' || r.reason === 'missing_personnel_relation').length;
    const totalFailed = allResults.filter(r => !r.success && r.reason !== 'no_events' && r.reason !== 'missing_personnel_relation').length;
    const totalTime = Math.round((Date.now() - startTime) / 1000);

    console.log(`\n✅ Regeneration complete in ${totalTime}s`);
    console.log(`   Total: ${totalRows} rows across ${pageCount} Calendar Data batches`);
    console.log(`   Success: ${totalSuccess}, Failed: ${totalFailed}, Skipped: ${totalSkipped}`);
    
    return { 
      success: true, 
      total: totalRows, 
      concurrency: concurrency,
      successCount: totalSuccess, 
      failCount: totalFailed, 
      skippedCount: totalSkipped, 
      results: allResults,
      timeSeconds: totalTime
    };
    
  } catch (error) {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.error(`❌ Error in batched regeneration after ${elapsed}s:`, error);
    return { success: false, error: error.message, timeSeconds: elapsed };
  }
}

// Background job to update all people continuously with a cooldown between cycles
let backgroundCycleSeq = 0;
let activeBackgroundCycles = 0;
let isBackgroundCycleRunning = false;
let activeManualRegens = 0;

async function waitForManualRegensToDrain(context = 'background cycle') {
  if (activeManualRegens <= 0) {
    return;
  }

  console.warn(`⏸️  Deferring ${context} while ${activeManualRegens} manual regen(s) are running`);
  while (activeManualRegens > 0) {
    await sleep(1000);
  }
}

function startBackgroundJob() {
  console.log(`🔄 Starting background calendar refresh job (run to completion, then wait ${Math.round(BACKGROUND_REFRESH_COOLDOWN_MS / 60000)} minutes)`);
  console.log(`   Processing all people with bounded workers (concurrency=${BACKGROUND_REGEN_CONCURRENCY}) each cycle`);
  console.log(`   Waiting ${BACKGROUND_INITIAL_DELAY_MS}ms before the first cycle so dependencies can finish connecting`);

  const scheduleNextCycle = (delayMs) => {
    setTimeout(runBackgroundCycle, Math.max(0, delayMs));
  };

  const runBackgroundCycle = async () => {
    const cycleId = `bg_${++backgroundCycleSeq}`;
    let cycleRegistered = false;
    try {
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'bg_refresh_trace',hypothesisId:'H1',location:'index.js:startBackgroundJob:intervalTick',message:'Background cycle fired',data:{cycleId,cooldownMs:BACKGROUND_REFRESH_COOLDOWN_MS,defaultConcurrency:DEFAULT_REGEN_CONCURRENCY,activeBackgroundCycles},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      if (isNotionCircuitOpen()) {
        const waitMs = Math.max(0, notionCircuitOpenUntil - Date.now());
        console.warn(`⏸️  Skipping background job while Notion circuit is open (${waitMs}ms remaining)`);
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'bg_refresh_trace',hypothesisId:'H4',location:'index.js:startBackgroundJob:circuitSkip',message:'Background cycle skipped by Notion circuit',data:{waitMs},timestamp:Date.now()})}).catch(()=>{});
        // #endregion
        return;
      }
      if (activeManualRegens > 0) {
        console.warn(`⏸️  Skipping background job while ${activeManualRegens} manual regen(s) are running`);
        return;
      }
      if (isBackgroundCycleRunning || activeBackgroundCycles > 0) {
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'post_fix',hypothesisId:'H5',location:'index.js:startBackgroundJob:overlapDetected',message:'Background cycle overlap detected and skipped',data:{cycleId,activeBackgroundCycles,isBackgroundCycleRunning},timestamp:Date.now()})}).catch(()=>{});
        // #endregion
        console.warn('⏭️  Background cycle request ignored because a previous cycle is still running');
        return;
      }
      isBackgroundCycleRunning = true;
      activeBackgroundCycles += 1;
      cycleRegistered = true;
      const jobStart = Date.now();
      let cyclePhase = 'calendar_data_sweep';
      verboseLog('\n⏰ Background job triggered - fetching Calendar Data rows...');
      verboseLog(`   Sweeping Calendar Data with pageSize=${CALENDAR_DATA_SWEEP_PAGE_SIZE} and concurrency=${BACKGROUND_REGEN_CONCURRENCY}`);

      const { results, totalRows, pageCount } = await processCalendarDataRowsPaginated({
        trigger: 'background_cycle',
        concurrency: BACKGROUND_REGEN_CONCURRENCY,
        maxRetries: 5,
        waitContext: `background cycle ${cycleId}`
      });

      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'bg_refresh_trace',hypothesisId:'H2',location:'index.js:startBackgroundJob:peopleLoaded',message:'Calendar Data rows loaded for background cycle',data:{count:totalRows,pageSize:CALENDAR_DATA_SWEEP_PAGE_SIZE,pageCount},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      
      if (totalRows === 0) {
        console.log('⚠️  No Calendar Data rows found in database');
        return;
      }

      const totalSuccess = results.filter(r => r.success).length;
      const totalSkipped = results.filter(r => r.reason === 'no_events' || r.reason === 'missing_personnel_relation').length;
      const totalFailed = results.filter(r => !r.success && r.reason !== 'no_events' && r.reason !== 'missing_personnel_relation').length;
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'eea81f'},body:JSON.stringify({sessionId:'eea81f',runId:'bg_cycle_debug',hypothesisId:'H2',location:'index.js:startBackgroundJob:afterSweep',message:'Background sweep completed before auxiliary refreshes',data:{cycleId,cyclePhase,totalRows,pageCount,totalSuccess,totalSkipped,totalFailed},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'post_fix',hypothesisId:'H3',location:'index.js:startBackgroundJob:cycleSummary',message:'Background Calendar Data cycle summary',data:{concurrency:BACKGROUND_REGEN_CONCURRENCY,total:totalRows,pageCount,totalSuccess,totalSkipped,totalFailed,elapsedMs:Date.now()-jobStart},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      const elapsedMs = Date.now() - jobStart;
      if (totalFailed > 0) {
        console.warn(`⚠️ Background Calendar Data cycle had failures: failed=${totalFailed}, success=${totalSuccess}, skipped=${totalSkipped}, total=${totalRows}, pageCount=${pageCount}, elapsedMs=${elapsedMs}`);
      }
      console.log(`✅ Background cycle ${cycleId} completed in ${elapsedMs}ms; next cycle in ${BACKGROUND_REFRESH_COOLDOWN_MS}ms`);
      
      // Also refresh admin calendar (allow up to 60s for slow Notion reads)
      if (ADMIN_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        cyclePhase = 'admin_refresh';
        try {
          verboseLog('🔄 Refreshing admin calendar...');
          const adminEvents = await withTimeout(
            getAdminCalendarData(),
            CALENDAR_FETCH_TIMEOUT_MS,
            `Admin calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
          );
          
          if (adminEvents && adminEvents.length > 0) {
            const allCalendarEvents = processAdminEvents(adminEvents);
            
            // Generate and cache ICS
            const calendar = ical({ 
              name: 'Admin Calendar',
              description: 'All upcoming events',
              ttl: 300
            });
            
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: []  // No alarms for admin calendar
              });
            });
            
            const icsData = serializeCalendar(calendar);
            await setCalendarCache('calendar:admin:ics', icsData);
            
            // Also cache JSON
            const jsonData = JSON.stringify({
              calendar_name: 'Admin Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await setCalendarCache('calendar:admin:json', jsonData);
            
            console.log(`✅ Admin calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (adminError) {
          console.error('⚠️  Admin calendar refresh failed:', adminError.message);
        }
      }
      
      // Also refresh travel calendar (allow up to 60s for slow Notion reads)
      if (TRAVEL_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        cyclePhase = 'travel_refresh';
        try {
          verboseLog('🔄 Refreshing travel calendar...');
          const travelEvents = await withTimeout(
            getTravelCalendarData(),
            CALENDAR_FETCH_TIMEOUT_MS,
            `Travel calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
          );
          
          if (travelEvents && travelEvents.length > 0) {
            const allCalendarEvents = processTravelEvents(travelEvents);
            
            // Generate and cache ICS
            const calendar = ical({ 
              name: 'Travel Calendar',
              description: 'All travel events',
              ttl: 300
            });
            
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: []  // No alarms for travel calendar
              });
            });
            
            const icsData = serializeCalendar(calendar);
            await setCalendarCache('calendar:travel:ics', icsData);
            
            // Also cache JSON
            const jsonData = JSON.stringify({
              calendar_name: 'Travel Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await setCalendarCache('calendar:travel:json', jsonData);
            
            console.log(`✅ Travel calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (travelError) {
          console.error('⚠️  Travel calendar refresh failed:', travelError.message);
        }
      }
      
      // Also refresh blockout calendar (allow up to 60s for slow Notion reads)
      if (BLOCKOUT_CALENDAR_PAGE_ID && redis && cacheEnabled) {
        cyclePhase = 'blockout_refresh';
        try {
          verboseLog('🔄 Refreshing blockout calendar...');
          const blockoutEvents = await withTimeout(
            getBlockoutCalendarData(),
            CALENDAR_FETCH_TIMEOUT_MS,
            `Blockout calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
          );
          if (blockoutEvents && blockoutEvents.length > 0) {
            const allCalendarEvents = processBlockoutEvents(blockoutEvents);
            const calendar = ical({ 
              name: 'Blockout Calendar',
              description: 'All blockout events',
              ttl: 300
            });
            allCalendarEvents.forEach(event => {
              const startDate = event.start instanceof Date ? event.start : new Date(event.start);
              const endDate = event.end instanceof Date ? event.end : new Date(event.end);
              calendar.createEvent({
                start: startDate,
                end: endDate,
                summary: event.title,
                description: event.description,
                location: event.location,
                url: event.url || '',
                floating: true,
                alarms: getAlarmsForEvent(event.type, event.title)
              });
            });
            const icsData = serializeCalendar(calendar);
            await setCalendarCache('calendar:blockout:ics', icsData);
            const jsonData = JSON.stringify({
              calendar_name: 'Blockout Calendar',
              total_events: allCalendarEvents.length,
              events: allCalendarEvents
            }, null, 2);
            await setCalendarCache('calendar:blockout:json', jsonData);
            console.log(`✅ Blockout calendar cached (${allCalendarEvents.length} events)`);
          }
        } catch (blockoutError) {
          console.error('⚠️  Blockout calendar refresh failed:', blockoutError.message);
        }
      }
      
      const jobTime = Math.round((Date.now() - jobStart) / 1000);
      cyclePhase = 'final_summary';
      // #region agent log
      fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'eea81f'},body:JSON.stringify({sessionId:'eea81f',runId:'bg_cycle_debug',hypothesisId:'H1',location:'index.js:startBackgroundJob:beforeFinalSummary',message:'About to log final background summary',data:{cycleId,cyclePhase,jobTime,totalSuccess,totalFailed,totalSkipped,totalRows},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      verboseLog(`✅ Background refresh complete in ${jobTime}s: ${totalSuccess} success, ${totalFailed} failed, ${totalSkipped} skipped (processed ${totalRows} total Calendar Data rows + admin calendar + travel calendar + blockout calendar)`);
      
    } catch (error) {
      console.error('❌ Background job error:', error.message);
      // #region agent log
      try {
        const cycleErrorPayload = {
          sessionId: 'b6a4e3',
          runId: 'bg_refresh_trace',
          hypothesisId: 'H6',
          location: 'index.js:startBackgroundJob:cycleError',
          message: 'Background cycle threw error',
          data: {
            cycleId,
            error: error?.message || 'unknown'
          },
          timestamp: Date.now()
        };
        fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': 'b6a4e3' },
          body: JSON.stringify(cycleErrorPayload)
        }).catch(() => {});
      } catch (_telemetryError) {
        // Never allow telemetry payload construction to crash the background loop.
      }
      // #endregion
    } finally {
      if (cycleRegistered) {
        activeBackgroundCycles = Math.max(0, activeBackgroundCycles - 1);
        isBackgroundCycleRunning = false;
        // #region agent log
        fetch('http://127.0.0.1:7242/ingest/32011d73-236e-46f0-b1c6-d2dcc17478a5',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'b6a4e3'},body:JSON.stringify({sessionId:'b6a4e3',runId:'post_fix',hypothesisId:'H5',location:'index.js:startBackgroundJob:cycleFinalize',message:'Background cycle finalized',data:{cycleId,activeBackgroundCycles,isBackgroundCycleRunning},timestamp:Date.now()})}).catch(()=>{});
        // #endregion
      }
      scheduleNextCycle(BACKGROUND_REFRESH_COOLDOWN_MS);
    }
  };

  scheduleNextCycle(BACKGROUND_INITIAL_DELAY_MS);
}

// ============================================
// ADMIN CALENDAR FUNCTIONS
// ============================================

// Helper function to get admin calendar data by page ID
async function getAdminCalendarData() {
  if (!ADMIN_CALENDAR_PAGE_ID) {
    throw new Error('ADMIN_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = ADMIN_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Admin Events property
  // Note: Using full page retrieve for now - property-specific retrieve may need property ID (UUID)
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Admin Events property
  const adminEventsString = page.properties['Admin Events']?.formula?.string || 
                            page.properties['Admin Events']?.rich_text?.[0]?.text?.content ||
                            '[]';

  try {
    const adminEvents = JSON.parse(adminEventsString);
    return Array.isArray(adminEvents) ? adminEvents : [];
  } catch (e) {
    console.error('Error parsing Admin Events JSON:', adminEventsString?.substring(0, 100));
    throw new Error(`Admin Events JSON parse error: ${e.message}`);
  }
}

// Helper function to process admin events into calendar format
function processAdminEvents(eventsArray) {
  const allCalendarEvents = [];

  const extractFirstHumanDateFromText = (raw) => {
    if (!raw || typeof raw !== 'string') return null;
    const match = raw.match(/([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})/);
    if (!match) return null;
    const monthName = match[1];
    const day = Number(match[2]);
    const year = Number(match[3]);
    const monthIndex = [
      'january', 'february', 'march', 'april', 'may', 'june',
      'july', 'august', 'september', 'october', 'november', 'december'
    ].indexOf(monthName.toLowerCase());
    if (monthIndex < 0) return null;
    return Date.UTC(year, monthIndex, day);
  };

  const alignEventTimesToHelperDate = (eventTimes, helperRaw) => {
    if (!eventTimes?.start || !helperRaw) {
      return { eventTimes, helperAdjusted: false, helperDeltaDays: 0 };
    }

    const helperDateUtc = extractFirstHumanDateFromText(helperRaw);
    if (helperDateUtc === null) {
      return { eventTimes, helperAdjusted: false, helperDeltaDays: 0 };
    }

    const startDateUtc = Date.UTC(
      eventTimes.start.getUTCFullYear(),
      eventTimes.start.getUTCMonth(),
      eventTimes.start.getUTCDate()
    );

    const deltaDays = Math.round((helperDateUtc - startDateUtc) / (24 * 60 * 60 * 1000));
    if (deltaDays === 0) {
      return { eventTimes, helperAdjusted: false, helperDeltaDays: 0 };
    }

    const deltaMs = deltaDays * 24 * 60 * 60 * 1000;
    return {
      eventTimes: {
        start: new Date(eventTimes.start.getTime() + deltaMs),
        end: new Date(eventTimes.end.getTime() + deltaMs)
      },
      helperAdjusted: true,
      helperDeltaDays: deltaDays
    };
  };

  eventsArray.forEach(event => {
    // Process main events (same logic as existing main_event processing)
    if (event.event_name && event.event_date) {
      const mainEventTimeResult = resolveMainEventTimes(event.event_date, event.calltime, {
        humanWallClock: true,
        atHumanNoConversion: true
      });
      const alignmentResult = alignEventTimesToHelperDate(
        mainEventTimeResult.eventTimes,
        event.event_date_helper
      );
      const eventTimes = alignmentResult.eventTimes;
      
      if (eventTimes) {
        // Build payroll info for description (put at TOP)
        let payrollInfo = '';
        
        const positionValue = typeof event.position === 'string' ? event.position.trim() : event.position;
        const assignmentsValue = typeof event.assignments === 'string' ? event.assignments.trim() : event.assignments;
        const payTotalRaw = event.pay_total;
        const payTotalStr = payTotalRaw === 0 ? '0' : (payTotalRaw ?? '').toString().trim();
        const hasPosition = positionValue !== undefined && positionValue !== null && `${positionValue}`.trim() !== '';
        const hasAssignments = assignmentsValue !== undefined && assignmentsValue !== null && `${assignmentsValue}`.trim() !== '';
        const hasPayTotal = payTotalRaw !== null && payTotalRaw !== undefined && payTotalStr !== '';

        if (hasPosition || hasAssignments || hasPayTotal) {
          if (hasPosition) {
            payrollInfo += `Position: ${positionValue}\n`;
          }
          if (hasAssignments) {
            payrollInfo += `Assignments: ${assignmentsValue}\n`;
          }
          if (hasPayTotal) {
            const payDisplay = payTotalStr.startsWith('$') ? payTotalStr : `$${payTotalStr}`;
            payrollInfo += `Pay: ${payDisplay}\n`;
          }
          payrollInfo += '\n---\n\n';
        }

        // Build description sections
        let description = payrollInfo; // Payroll info at top
        
        // Calltime
        if (event.calltime) {
          description += `🕐 Calltime: ${formatCallTime(event.calltime)}\n`;
        }
        
        // Gear
        if (event.gear) {
          description += `🎸 Gear: ${event.gear}\n`;
        }
        
        // Personnel (handle both string and array formats)
        if (event.event_personnel) {
          if (typeof event.event_personnel === 'string') {
            // Split by newlines if it's a string
            description += `\n👥 Personnel:\n${event.event_personnel}\n`;
          } else if (Array.isArray(event.event_personnel) && event.event_personnel.length > 0) {
            description += `\n👥 Personnel:\n`;
            event.event_personnel.forEach(person => {
              description += `  • ${person}\n`;
            });
          }
        }
        
        // General Info / Notes
        if (event.general_info) {
          description += `\n📋 General Info:\n${event.general_info}\n`;
        }
        
        // Notion URL is in URL field, not in description

        const rawTitle = typeof event.event_name === 'string' ? event.event_name.trim() : '';
        const title = rawTitle ? `🎸 ${rawTitle}` : '🎸 Event';

        // Build location from venue and venue_address
        let location = '';
        if (event.venue && event.venue_address) {
          location = `${event.venue}, ${event.venue_address}`;
        } else if (event.venue_address) {
          location = event.venue_address;
        } else if (event.venue) {
          location = event.venue;
        }

        allCalendarEvents.push({
          start: eventTimes.start,
          end: eventTimes.end,
          title: title,
          description: description.trim(),
          location: location,
          url: event.notion_url || '',
          type: 'main_event',
          helperAdjusted: alignmentResult.helperAdjusted,
          helperDeltaDays: alignmentResult.helperDeltaDays
        });
      }
    }

    // Process rehearsals for this event
    const nestedRehearsals = getNestedRehearsals(event);
    if (nestedRehearsals.length > 0) {
      nestedRehearsals.forEach(rehearsal => {
        if (rehearsal.rehearsal_time && rehearsal.rehearsal_time !== null) {
          let rehearsalTimes = parseUnifiedDateTime(rehearsal.rehearsal_time);
          
          if (rehearsalTimes) {
            // Build location from rehearsal_location and rehearsal_address
            let location = 'TBD';
            if (rehearsal.rehearsal_location && rehearsal.rehearsal_address) {
              location = `${rehearsal.rehearsal_location}, ${rehearsal.rehearsal_address}`;
            } else if (rehearsal.rehearsal_location) {
              location = rehearsal.rehearsal_location;
            } else if (rehearsal.rehearsal_address) {
              location = rehearsal.rehearsal_address.trim().replace(/\u2060/g, '');
            }

            // Build description
            let description = rehearsal.description || `Rehearsal`;
            if (rehearsal.rehearsal_band) {
              description += `\n\nBand Personnel:\n${rehearsal.rehearsal_band}`;
            }

            // Build title with event name and band if available
            let title = `🎤 Rehearsal - ${event.event_name || 'Event'}`;
            if (event.band) {
              title += ` (${event.band})`;
            }

            allCalendarEvents.push({
              type: 'rehearsal',
              title: title,
              start: rehearsalTimes.start,
              end: rehearsalTimes.end,
              description: description,
              location: location,
              url: rehearsal.rehearsal_link || '',
              mainEvent: event.event_name || ''
            });
          }
        }
      });
    }
  });

  return allCalendarEvents;
}

// ============================================
// TRAVEL CALENDAR FUNCTIONS
// ============================================

// Helper function to get travel calendar data by page ID
async function getTravelCalendarData() {
  if (!TRAVEL_CALENDAR_PAGE_ID) {
    throw new Error('TRAVEL_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = TRAVEL_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Travel Admin property
  // Note: Using full page retrieve for now - property-specific retrieve may need property ID (UUID)
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Travel Admin property
  let travelEventsString = page.properties['Travel Admin']?.formula?.string || 
                          page.properties['Travel Admin']?.rich_text?.[0]?.text?.content ||
                          '[]';

  // Clean the string - remove any leading/trailing whitespace
  travelEventsString = travelEventsString.trim();

  // Extract only the first complete top-level JSON container (object/array).
  // This tolerates Notion formulas that append extra non-JSON text.
  const extractFirstJsonContainer = (input) => {
    const startObj = input.indexOf('{');
    const startArr = input.indexOf('[');
    if (startObj === -1 && startArr === -1) return input;

    let start = -1;
    if (startObj === -1) start = startArr;
    else if (startArr === -1) start = startObj;
    else start = Math.min(startObj, startArr);

    const openChar = input[start];
    const closeChar = openChar === '{' ? '}' : ']';

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let i = start; i < input.length; i++) {
      const ch = input[i];

      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }
        if (ch === '\\') {
          escaped = true;
          continue;
        }
        if (ch === '"') {
          inString = false;
        }
        continue;
      }

      if (ch === '"') {
        inString = true;
        continue;
      }

      if (ch === openChar) depth += 1;
      if (ch === closeChar) {
        depth -= 1;
        if (depth === 0) {
          return input.slice(start, i + 1);
        }
      }
    }

    return input.slice(start);
  };

  travelEventsString = extractFirstJsonContainer(travelEventsString);

  // Fix double commas (common JSON formatting issue)
  travelEventsString = travelEventsString.replace(/,,+/g, ',');
  
  // Fix malformed arrays where "personnel_ids" appears as a key inside the personnel array
  // Pattern: ["Name1","Name2","personnel_ids":[...] should become ["Name1","Name2"]
  // This handles cases where personnel_ids key appears inside the personnel array
  travelEventsString = travelEventsString.replace(/"personnel":\[([^\]]*)"personnel_ids":/g, (match, personnelList) => {
    // Remove any trailing commas from personnel list
    const cleaned = personnelList.replace(/,\s*$/, '');
    return `"personnel":[${cleaned}]`;
  });

  // Notion formulas can emit literal control characters (newlines/tabs) inside
  // JSON string values. Escape those so JSON.parse can handle the payload.
  const escapeControlCharsInJsonStrings = (input) => {
    let out = '';
    let inString = false;
    let escaped = false;

    for (let i = 0; i < input.length; i++) {
      const ch = input[i];

      if (inString) {
        if (escaped) {
          out += ch;
          escaped = false;
          continue;
        }
        if (ch === '\\') {
          out += ch;
          escaped = true;
          continue;
        }
        if (ch === '"') {
          out += ch;
          inString = false;
          continue;
        }
        if (ch === '\n') {
          out += '\\n';
          continue;
        }
        if (ch === '\r') {
          out += '\\r';
          continue;
        }
        if (ch === '\t') {
          out += '\\t';
          continue;
        }
        out += ch;
        continue;
      }

      if (ch === '"') {
        inString = true;
      }
      out += ch;
    }

    return out;
  };

  travelEventsString = escapeControlCharsInJsonStrings(travelEventsString);

  try {
    const travelEvents = JSON.parse(travelEventsString);

    // Legacy support: accept either an array of travel groups, or a single
    // travel group object with flights/hotels/ground_transportation.
    if (Array.isArray(travelEvents)) {
      return travelEvents;
    }
    if (
      travelEvents &&
      typeof travelEvents === 'object' &&
      (
        Array.isArray(travelEvents.flights) ||
        Array.isArray(travelEvents.hotels) ||
        Array.isArray(travelEvents.ground_transportation)
      )
    ) {
      return [travelEvents];
    }
    return [];
  } catch (e) {
    console.error('Error parsing Travel Admin JSON. First 200 chars:', travelEventsString?.substring(0, 200));
    console.error('Full length:', travelEventsString?.length);
    console.error('Parse error:', e.message);
    // Try to find the problematic area
    const errorPos = parseInt(e.message.match(/position (\d+)/)?.[1]) || 0;
    if (errorPos > 0) {
      const start = Math.max(0, errorPos - 100);
      const end = Math.min(travelEventsString.length, errorPos + 100);
      console.error('Problem area:', travelEventsString.substring(start, end));
    }
    throw new Error(`Travel Admin JSON parse error: ${e.message}. First 200 chars: ${travelEventsString?.substring(0, 200)}`);
  }
}

// Helper function to process travel events into calendar format
function processTravelEvents(travelGroupsArray) {
  const allCalendarEvents = [];
  // Legacy schema only: each element is a travel group with
  // flights, hotels, and ground_transportation arrays.
  travelGroupsArray.forEach(travelGroup => {
    // Process flights
    if (travelGroup.flights && Array.isArray(travelGroup.flights)) {
      travelGroup.flights.forEach(flight => {
        // Departure flight
        if (flight.departure_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const depTimes = parseUnifiedDateTime(flight.departure_time);
          const depEndTimes = flight.departure_arrival_time
            ? parseUnifiedDateTime(flight.departure_arrival_time)
            : null;
          const depStart = depTimes ? depTimes.start : new Date(flight.departure_time);
          const depEnd = depEndTimes
            ? depEndTimes.end
            : (depTimes ? depTimes.end : new Date(flight.departure_time));
          
          if (!isNaN(depStart.getTime()) && !isNaN(depEnd.getTime())) {
            // Build route string
            const routeFrom = flight.departure_from_city || flight.departure_from || '';
            const routeTo = flight.departure_to_city || flight.departure_to || '';
            const route = routeFrom && routeTo ? `${routeFrom} → ${routeTo}` : '';
            
            // Build title - prefer departure_name, fallback to route
            let title = flight.departure_name || (route ? `Flight: ${route}` : 'Flight');
            
            // Build structured description
            let description = '';
            
            if (flight.departure_airline && flight.departure_flightnumber) {
              description += `✈️ ${flight.departure_airline} ${flight.departure_flightnumber}\n`;
            }
            
            if (route) {
              description += `Route: ${route}\n`;
            }
            
            // Personnel
            if (flight.personnel && flight.personnel.personnel_name && Array.isArray(flight.personnel.personnel_name) && flight.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              flight.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (flight.confirmation || flight.flight_status) {
              description += `\n📋 Booking Details:\n`;
              if (flight.confirmation) {
                description += `   Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                description += `   Status: ${flight.flight_status}\n`;
              }
            }

            const location = flight.departure_from_city 
              ? `${flight.departure_from} (${flight.departure_from_city})`
              : flight.departure_from || '';

            // Prefer the explicit flight URL from the formula, but keep legacy support.
            const url = flight.flight_url || flight.notion_url || '';

            allCalendarEvents.push({
              start: depStart,
              end: depEnd,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'flight_departure'
            });
          }
        }

        // Return flight
        if (flight.return_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const retTimes = parseUnifiedDateTime(flight.return_time);
          const retEndTimes = flight.return_arrival_time
            ? parseUnifiedDateTime(flight.return_arrival_time)
            : null;
          const retStart = retTimes ? retTimes.start : new Date(flight.return_time);
          const retEnd = retEndTimes
            ? retEndTimes.end
            : (retTimes ? retTimes.end : new Date(flight.return_time));
          
          if (!isNaN(retStart.getTime()) && !isNaN(retEnd.getTime())) {
            // Build route string
            const routeFrom = flight.return_from_city || flight.return_from || '';
            const routeTo = flight.return_to_city || flight.return_to || '';
            const route = routeFrom && routeTo ? `${routeFrom} → ${routeTo}` : '';
            
            // Build title - prefer return_name, fallback to route
            let title = flight.return_name || (route ? `Flight Return: ${route}` : 'Flight Return');
            
            // Build structured description
            let description = '';
            
            if (flight.return_airline && flight.return_flightnumber) {
              description += `✈️ ${flight.return_airline} ${flight.return_flightnumber}\n`;
            }
            
            if (route) {
              description += `Route: ${route}\n`;
            }
            
            // Personnel
            if (flight.personnel && flight.personnel.personnel_name && Array.isArray(flight.personnel.personnel_name) && flight.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              flight.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            if (flight.return_confirmation || flight.confirmation || flight.flight_status) {
              description += `\n📋 Booking Details:\n`;
              if (flight.return_confirmation) {
                description += `   Confirmation: ${flight.return_confirmation}\n`;
              } else if (flight.confirmation) {
                description += `   Confirmation: ${flight.confirmation}\n`;
              }
              if (flight.flight_status) {
                description += `   Status: ${flight.flight_status}\n`;
              }
            }

            const location = flight.return_from_city 
              ? `${flight.return_from} (${flight.return_from_city})`
              : flight.return_from || '';

            // Prefer the explicit flight URL from the formula, but keep legacy support.
            const url = flight.flight_url || flight.notion_url || '';

            allCalendarEvents.push({
              start: retStart,
              end: retEnd,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'flight_return'
            });
          }
        }
      });
    }

    // Process hotels
    if (travelGroup.hotels && Array.isArray(travelGroup.hotels)) {
      travelGroup.hotels.forEach(hotel => {
        // Keep backward compatibility with both the new hotel_url and legacy notion_url fields.
        if (!hotel.hotel_url && hotel.notion_url) {
          hotel.hotel_url = hotel.notion_url;
        }
        if (!hotel.hotel_url && travelGroup.notion_url) {
          hotel.hotel_url = travelGroup.notion_url;
        }
        // Extract location from title (e.g., "Hotel - North Beach ()" -> "North Beach")
        let locationName = '';
        if (hotel.title) {
          const titleMatch = hotel.title.match(/Hotel\s*-\s*([^(]+)/);
          if (titleMatch) {
            locationName = titleMatch[1].trim();
          }
        }
        
        // Build better title: "Hotel: [Hotel Name]" or "Hotel - [Location]: [Hotel Name]"
        let title = '';
        if (hotel.hotel_name) {
          if (locationName) {
            title = `Hotel - ${locationName}: ${hotel.hotel_name}`;
          } else {
            title = `Hotel: ${hotel.hotel_name}`;
          }
        } else if (locationName) {
          title = `Hotel - ${locationName}`;
        } else {
          title = hotel.title || 'Hotel';
        }

        // Support both legacy check_in/check_out fields and dates_booked range.
        const bookedRange = hotel.dates_booked ? parseUnifiedDateTime(hotel.dates_booked) : null;
        const parsedCheckIn = hotel.check_in ? parseUnifiedDateTime(hotel.check_in) : null;
        const parsedCheckOut = hotel.check_out ? parseUnifiedDateTime(hotel.check_out) : null;
        const checkInDate = parsedCheckIn ? parsedCheckIn.start : (hotel.check_in ? new Date(hotel.check_in) : (bookedRange ? bookedRange.start : null));
        const checkOutDate = parsedCheckOut ? parsedCheckOut.start : (hotel.check_out ? new Date(hotel.check_out) : (bookedRange ? bookedRange.end : null));
        
        // Hotel check-in
        if (checkInDate) {
          const checkIn = checkInDate;
          if (!isNaN(checkIn.getTime())) {
            let description = '';
            
            // Hotel name and location
            if (hotel.hotel_name) {
              description += `🏨 ${hotel.hotel_name}\n`;
            }
            if (locationName) {
              description += `📍 ${locationName}\n`;
            }
            
            // Personnel
            if (hotel.personnel && hotel.personnel.personnel_name && Array.isArray(hotel.personnel.personnel_name) && hotel.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              hotel.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            // Booking details section
            const reservationNames = typeof hotel.names_on_reservation === 'string'
              ? hotel.names_on_reservation.split(',').map(name => name.trim()).filter(Boolean)
              : [];
            const hasBookingDetails = hotel.confirmation || hotel.name_under_reservation || hotel.hotel_phone || reservationNames.length > 0;
            if (hasBookingDetails) {
              description += `\n📋 Booking Details:\n`;
              if (hotel.confirmation) {
                description += `   Confirmation: ${hotel.confirmation}\n`;
              }
              if (reservationNames.length > 0) {
                description += `   Names on Reservation:\n`;
                reservationNames.forEach(name => {
                  description += `   - ${name}\n`;
                });
              }
              if (hotel.name_under_reservation) {
                description += `   Reservation: ${hotel.name_under_reservation}\n`;
              }
              if (hotel.hotel_phone) {
                description += `   Phone: ${hotel.hotel_phone}\n`;
              }
            }
            
            // Dates section
            if (checkOutDate) {
              const checkOutDesc = checkOutDate;
              if (!isNaN(checkOutDesc.getTime())) {
                const nights = Math.ceil((checkOutDesc - checkIn) / (1000 * 60 * 60 * 24));
                description += `\n📅 Dates:\n`;
                description += `   Check-out: ${checkOutDesc.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}\n`;
                description += `   Duration: ${nights} night${nights !== 1 ? 's' : ''}\n`;
              }
            }
            
            // Maps section (only if we have maps links)
            if (hotel.hotel_apple_maps || hotel.hotel_google_maps) {
              description += `\n🗺️ Maps:\n`;
              if (hotel.hotel_apple_maps) {
                description += `   Apple Maps: ${hotel.hotel_apple_maps}\n`;
              }
              if (hotel.hotel_google_maps) {
                description += `   Google Maps: ${hotel.hotel_google_maps}\n`;
              }
            }

            // Use actual check-in and check-out times
            const checkOutForEvent = checkOutDate && !isNaN(checkOutDate.getTime())
              ? checkOutDate
              : new Date(checkIn.getTime() + 24 * 60 * 60 * 1000);
            
            // Location field: combine hotel name and address
            let location = '';
            if (hotel.hotel_name && hotel.hotel_address) {
              location = `${hotel.hotel_name} ${hotel.hotel_address}`;
            } else if (hotel.hotel_address) {
              location = hotel.hotel_address;
            } else if (hotel.hotel_name) {
              location = hotel.hotel_name;
            }

            // Prefer the explicit hotel URL from the formula, but keep legacy support.
            const url = hotel.hotel_url || hotel.notion_url || '';

            allCalendarEvents.push({
              start: checkIn,
              end: checkOutForEvent,
              title: title,
              description: description.trim(),
              location: location,
              url: url,
              type: 'hotel_checkin'
            });
          }
        }

        // Hotel check-out
        if (checkOutDate) {
          const checkOut = checkOutDate;
          if (!isNaN(checkOut.getTime())) {
            let description = '';
            
            if (hotel.hotel_name) {
              description += `🏨 ${hotel.hotel_name} - Check-out\n`;
            }
            
            // Personnel
            if (hotel.personnel && hotel.personnel.personnel_name && Array.isArray(hotel.personnel.personnel_name) && hotel.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              hotel.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }
            
            const reservationNames = typeof hotel.names_on_reservation === 'string'
              ? hotel.names_on_reservation.split(',').map(name => name.trim()).filter(Boolean)
              : [];
            if (hotel.confirmation || reservationNames.length > 0) {
              description += `\n📋 Booking Details:\n`;
              if (hotel.confirmation) {
                description += `   Confirmation: ${hotel.confirmation}\n`;
              }
              if (reservationNames.length > 0) {
                description += `   Names on Reservation:\n`;
                reservationNames.forEach(name => {
                  description += `   - ${name}\n`;
                });
              }
            }

            // Location field: combine hotel name and address
            let location = '';
            if (hotel.hotel_name && hotel.hotel_address) {
              location = `${hotel.hotel_name} ${hotel.hotel_address}`;
            } else if (hotel.hotel_address) {
              location = hotel.hotel_address;
            } else if (hotel.hotel_name) {
              location = hotel.hotel_name;
            }

            // Prefer the explicit hotel URL from the formula, but keep legacy support.
            const url = hotel.hotel_url || hotel.notion_url || '';

            allCalendarEvents.push({
              start: checkOut,
              end: new Date(checkOut.getTime() + 60 * 60 * 1000), // 1 hour event
              title: hotel.hotel_name ? `${hotel.hotel_name} - Check-out` : 'Hotel Check-out',
              description: description.trim(),
              location: location,
              url: url,
              type: 'hotel_checkout'
            });
          }
        }
      });
    }

    // Process ground transportation
    if (travelGroup.ground_transportation && Array.isArray(travelGroup.ground_transportation)) {
      travelGroup.ground_transportation.forEach(transport => {
        // Pickup event
        if (transport.pickup_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const pickupTimes = parseUnifiedDateTime(transport.pickup_time);
          const pickupTime = pickupTimes ? pickupTimes.start : new Date(transport.pickup_time);
          if (!isNaN(pickupTime.getTime())) {
            let description = '';

            if (transport.event_name) {
              description += `🎉 Event: ${transport.event_name}\n`;
            }
            
            if (transport.transportation_name) {
              description += `🚗 ${transport.transportation_name}\n`;
            }
            
            if (transport.pickup_name) {
              description += `Pickup: ${transport.pickup_name}\n`;
            }
            
            // Personnel
            if (transport.personnel && transport.personnel.personnel_name && Array.isArray(transport.personnel.personnel_name) && transport.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              transport.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }

            if (Array.isArray(transport.drivers) && transport.drivers.length > 0) {
              description += `\n🚘 Drivers:\n`;
              transport.drivers.forEach(driver => {
                if (driver && typeof driver === 'string') {
                  description += `${driver}\n`;
                }
              });
            }

            if (Array.isArray(transport.passengers) && transport.passengers.length > 0) {
              description += `\n🧳 Passengers:\n`;
              transport.passengers.forEach(passenger => {
                if (passenger && typeof passenger === 'string') {
                  description += `${passenger}\n`;
                }
              });
            }
            
            if (transport.confirmation) {
              description += `\n📋 Confirmation: ${transport.confirmation}\n`;
            }
            
            if (transport.pickup_address) {
              description += `\n📍 ${transport.pickup_address}`;
            }
            
            if (transport.pickup_address_apple) {
              description += `\n🗺️ Apple Maps: ${transport.pickup_address_apple}`;
            }
            
            if (transport.pickup_address_google) {
              description += `\n🗺️ Google Maps: ${transport.pickup_address_google}`;
            }

            const location = transport.pickup_name || transport.pickup_address || '';
            const url = transport.transportation_url ||
              transport.url ||
              transport.notion_url ||
              transport.pickup_address_google ||
              transport.pickup_address_apple ||
              '';

            allCalendarEvents.push({
              start: pickupTime,
              end: new Date(pickupTime.getTime() + 30 * 60 * 1000), // 30 minute event
              title: transport.transportation_name || transport.title || `Transportation Pickup: ${transport.pickup_name || 'Pickup'}`,
              description: description.trim(),
              location: location,
              url: url,
              type: 'transportation_pickup'
            });
          }
        }

        // Drop-off event
        if (transport.drop_off_time) {
          // Use parseUnifiedDateTime for proper UTC to Pacific conversion
          const dropOffTimes = parseUnifiedDateTime(transport.drop_off_time);
          const dropOffTime = dropOffTimes ? dropOffTimes.start : new Date(transport.drop_off_time);
          if (!isNaN(dropOffTime.getTime())) {
            let description = '';

            if (transport.event_name) {
              description += `🎉 Event: ${transport.event_name}\n`;
            }
            
            if (transport.transportation_name) {
              description += `🚗 ${transport.transportation_name} - Drop-off\n`;
            }
            
            if (transport.drop_off_name) {
              description += `Drop-off: ${transport.drop_off_name}\n`;
            }
            
            // Personnel
            if (transport.personnel && transport.personnel.personnel_name && Array.isArray(transport.personnel.personnel_name) && transport.personnel.personnel_name.length > 0) {
              description += `\n👥 Personnel:\n`;
              transport.personnel.personnel_name.forEach(person => {
                if (person && typeof person === 'string') {
                  description += `${person}\n`;
                }
              });
            }

            if (Array.isArray(transport.drivers) && transport.drivers.length > 0) {
              description += `\n🚘 Drivers:\n`;
              transport.drivers.forEach(driver => {
                if (driver && typeof driver === 'string') {
                  description += `${driver}\n`;
                }
              });
            }

            if (Array.isArray(transport.passengers) && transport.passengers.length > 0) {
              description += `\n🧳 Passengers:\n`;
              transport.passengers.forEach(passenger => {
                if (passenger && typeof passenger === 'string') {
                  description += `${passenger}\n`;
                }
              });
            }
            
            if (transport.confirmation) {
              description += `\n📋 Confirmation: ${transport.confirmation}\n`;
            }
            
            if (transport.drop_off_address) {
              description += `\n📍 ${transport.drop_off_address}`;
            }

            const location = transport.drop_off_name || transport.drop_off_address || '';
            const url = transport.transportation_url ||
              transport.url ||
              transport.notion_url ||
              transport.drop_off_address_google ||
              transport.drop_off_address_apple ||
              '';

            allCalendarEvents.push({
              start: dropOffTime,
              end: new Date(dropOffTime.getTime() + 30 * 60 * 1000), // 30 minute event
              title: transport.transportation_name
                ? `${transport.transportation_name} - Drop-off`
                : (transport.title || `Transportation Drop-off: ${transport.drop_off_name || 'Drop-off'}`),
              description: description.trim(),
              location: location,
              url: url,
              type: 'transportation_dropoff'
            });
          }
        }
      });
    }
  });

  return allCalendarEvents;
}

// ============================================
// BLOCKOUT CALENDAR FUNCTIONS
// ============================================

// Helper function to get blockout calendar data by page ID
async function getBlockoutCalendarData() {
  if (!BLOCKOUT_CALENDAR_PAGE_ID) {
    throw new Error('BLOCKOUT_CALENDAR_PAGE_ID not configured');
  }

  // Format the page ID properly (add dashes if needed)
  let pageId = BLOCKOUT_CALENDAR_PAGE_ID;
  if (pageId.length === 32 && !pageId.includes('-')) {
    pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  }

  // Fetch the page and extract Blockout Admin property
  const page = await retryNotionCall(() => 
    notion.pages.retrieve({ page_id: pageId })
  );

  // Extract Blockout Admin property
  let blockoutEventsString = page.properties['Blockout Admin']?.formula?.string || 
                            page.properties['Blockout Admin']?.rich_text?.[0]?.text?.content ||
                            '[]';

  // Clean the string - remove any leading/trailing whitespace
  blockoutEventsString = blockoutEventsString.trim();

  // Try to extract JSON if there's extra text (look for first [ and last ])
  if (blockoutEventsString.includes('[') && blockoutEventsString.includes(']')) {
    const firstBracket = blockoutEventsString.indexOf('[');
    const lastBracket = blockoutEventsString.lastIndexOf(']');
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      blockoutEventsString = blockoutEventsString.substring(firstBracket, lastBracket + 1);
    }
  }

  // Fix double commas (common JSON formatting issue)
  blockoutEventsString = blockoutEventsString.replace(/,,+/g, ',');

  try {
    const blockoutEvents = JSON.parse(blockoutEventsString);
    return Array.isArray(blockoutEvents) ? blockoutEvents : [];
  } catch (e) {
    console.error('Error parsing Blockout Admin JSON. First 200 chars:', blockoutEventsString?.substring(0, 200));
    console.error('Full length:', blockoutEventsString?.length);
    console.error('Parse error:', e.message);
    throw new Error(`Blockout Admin JSON parse error: ${e.message}. First 200 chars: ${blockoutEventsString?.substring(0, 200)}`);
  }
}

// Helper function to process blockout events into calendar format
function processBlockoutEvents(eventsArray) {
  const allCalendarEvents = [];

  eventsArray.forEach(event => {
    // Blockout events have: personnel_name, date_start, date_end, reason, notion_url
    if (event.personnel_name && event.date_start && event.date_end) {
      try {
        // Parse dates (format: YYYY-MM-DD)
        const startDate = new Date(event.date_start + 'T00:00:00');
        const endDate = new Date(event.date_end + 'T23:59:59');
        
        // If end date is same as start date, make it a single day event
        if (event.date_start === event.date_end) {
          endDate.setHours(23, 59, 59);
        }
        
        if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          // Description: Only include reason if available
          // (Personnel name is in title, Notion URL is in URL field)
          let description = '';
          if (event.reason && event.reason.trim()) {
            description = event.reason.trim();
          }

          // Title: "Blockout: [Personnel Name]"
          const title = `Blockout: ${event.personnel_name}`;

          allCalendarEvents.push({
            start: startDate,
            end: endDate,
            title: title,
            description: description,
            location: '',
            url: event.notion_url || '',
            type: 'blockout_event'
          });
        }
      } catch (dateError) {
        console.error('Error parsing blockout event dates:', dateError, event);
      }
    }
  });

  return allCalendarEvents;
}

// Debug endpoint for blockout calendar
app.get('/debug/blockout', async (req, res) => {
  try {
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    // Format the page ID properly
    let pageId = BLOCKOUT_CALENDAR_PAGE_ID;
    if (pageId.length === 32 && !pageId.includes('-')) {
      pageId = pageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Fetch the page
    const page = await retryNotionCall(() => 
      notion.pages.retrieve({ page_id: pageId })
    );

    // Get all properties to see what's available
    const availableProperties = Object.keys(page.properties || {});
    
    // Try to get Blockout Admin property
    const blockoutAdminProperty = page.properties['Blockout Admin'];
    
    // Get the raw value
    let blockoutEventsString = '';
    let propertyType = 'unknown';
    
    if (blockoutAdminProperty) {
      propertyType = blockoutAdminProperty.type;
      if (blockoutAdminProperty.formula?.type === 'string') {
        blockoutEventsString = blockoutAdminProperty.formula.string || '';
      } else if (blockoutAdminProperty.rich_text?.length > 0) {
        blockoutEventsString = blockoutAdminProperty.rich_text[0].text?.content || '';
      } else if (blockoutAdminProperty.formula) {
        blockoutEventsString = JSON.stringify(blockoutAdminProperty.formula);
      }
    }

    res.json({
      pageId: pageId,
      pageTitle: page.properties?.Name?.title?.[0]?.text?.content || 
                page.properties?.Title?.title?.[0]?.text?.content ||
                'Unknown',
      availableProperties: availableProperties,
      blockoutAdminFound: !!blockoutAdminProperty,
      blockoutAdminType: propertyType,
      blockoutAdminValueLength: blockoutEventsString?.length || 0,
      blockoutAdminPreview: blockoutEventsString?.substring(0, 500) || 'Empty or not found',
      fullBlockoutAdminValue: blockoutEventsString || null
    });
  } catch (error) {
    console.error('Error debugging blockout calendar:', error);
    res.status(500).json({ 
      error: 'Error debugging blockout calendar',
      message: error.message,
      details: error.stack
    });
  }
});

// Health check endpoint
app.get('/', (_req, res) => {
  res.json({
    status: `Calendar Feed Server Running (Cache ${cacheEnabled ? 'Enabled' : 'Disabled'})`,
    version: 'tz-fix-v9-calltime-facevalue',
    endpoints: {
      subscribe: '/subscribe/:personId',
      calendar: '/calendar/:personId',
      ics: '/calendar/:personId?format=ics',
      debug: '/debug/simple-test/:personId',
      debug_calendar_data: '/debug/calendar-data/:personId',
      cache_clear: '/cache/clear/:personId',
      cache_clear_all: '/cache/clear-all',
      regenerate_one: '/regenerate/:personId',
      regenerate_all: '/regenerate-all'
    },
    cache: {
      ttl: 'persistent (until next refresh or manual clear)',
      status: cacheEnabled ? 'enabled' : 'disabled'
    },
    backgroundJob: {
      status: 'running',
      interval: `runs continuously with ${Math.round(BACKGROUND_REFRESH_COOLDOWN_MS / 60000)} minute cooldown`,
      description: `Updates all people after each cycle completes, then waits ${Math.round(BACKGROUND_REFRESH_COOLDOWN_MS / 60000)} minutes`
    }
  });
});

// Flight countdown API endpoint - Direct Notion Query
app.get('/api/flight/:flightId', async (req, res) => {
  try {
    const { flightId } = req.params;
    
    // Special case for test flight
    if (flightId === 'test') {
      return res.json({
        flightNumber: 'AS 1360',
        departureTime: '2025-01-15T19:59:00.000Z/2025-01-15T21:34:00.000Z',
        airline: 'Alaska',
        route: 'LAX-SJD',
        confirmation: 'TEST123',
        departureCode: 'LAX',
        arrivalCode: 'SJD',
        departureName: 'Los Angeles International Airport',
        arrivalName: 'Los Cabos International Airport'
      });
    }
    
    // Parse flightId: {notionPageId}-{direction}
    const parts = flightId.split('-');
    if (parts.length < 2) {
      return res.status(400).json({ error: 'Invalid flight ID format' });
    }
    
    const direction = parts.pop();
    let notionPageId = parts.join('-');
    
    // Convert 32-character page ID to UUID format if needed
    if (notionPageId.length === 32 && !notionPageId.includes('-')) {
      notionPageId = notionPageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }
    
    if (!['departure', 'return'].includes(direction)) {
      return res.status(400).json({ error: 'Invalid direction. Must be "departure" or "return"' });
    }
    
    // Query Notion page
    const page = await notion.pages.retrieve({ page_id: notionPageId });
    const properties = page.properties;
    
    // Debug: Log available properties
    console.log('Available properties:', Object.keys(properties));
    
    // Temporary: Return available properties for debugging
    if (req.query.debug === 'true') {
      return res.json({
        notionPageId,
        direction,
        availableProperties: Object.keys(properties),
        properties: properties
      });
    }
    
    // Extract flight data based on direction
    let flightData;
    if (direction === 'departure') {
      // Parse departure time range
      const departureTime = properties.departure_time?.date?.start;
      const departureArrivalTime = properties.departure_arrival_time?.date?.start;
      
      if (!departureTime) {
        return res.status(404).json({ error: 'Departure time not found' });
      }
      
      const departureTimeRange = departureArrivalTime
        ? `${departureTime}/${departureArrivalTime}`
        : `${departureTime}/${departureTime}`;
      
      flightData = {
        flightNumber: properties.departure_flightnumber?.title?.[0]?.text?.content || 'N/A',
        departureTime: departureTimeRange,
        airline: properties.departure_airline?.select?.name || 'N/A',
        route: `${properties.departure_airport?.select?.name || 'N/A'}-${properties.return_airport?.select?.name || 'N/A'}`,
        confirmation: properties.confirmation?.rich_text?.[0]?.text?.content || 'N/A',
        departureCode: properties.departure_airport?.select?.name || 'N/A',
        arrivalCode: properties.return_airport?.select?.name || 'N/A',
        departureName: properties.departure_airport_name?.rich_text?.[0]?.text?.content || 'N/A',
        arrivalName: properties.return_airport_name?.rich_text?.[0]?.text?.content || 'N/A'
      };
    } else {
      // Parse return time range
      const returnTime = properties.return_time?.date?.start;
      const returnArrivalTime = properties.return_arrival_time?.date?.start;
      
      if (!returnTime) {
        return res.status(404).json({ error: 'Return time not found' });
      }
      
      const returnTimeRange = returnArrivalTime
        ? `${returnTime}/${returnArrivalTime}`
        : `${returnTime}/${returnTime}`;
      
      flightData = {
        flightNumber: properties.return_flightnumber?.title?.[0]?.text?.content || 'N/A',
        departureTime: returnTimeRange,
        airline: properties.return_airline?.select?.name || properties.departure_airline?.select?.name || 'N/A',
        route: `${properties.return_airport?.select?.name || 'N/A'}-${properties.departure_airport?.select?.name || 'N/A'}`,
        confirmation: properties.confirmation?.rich_text?.[0]?.text?.content || 'N/A',
        departureCode: properties.return_airport?.select?.name || 'N/A',
        arrivalCode: properties.departure_airport?.select?.name || 'N/A',
        departureName: properties.return_airport_name?.rich_text?.[0]?.text?.content || 'N/A',
        arrivalName: properties.departure_airport_name?.rich_text?.[0]?.text?.content || 'N/A'
      };
    }
    
    res.json(flightData);
  } catch (error) {
    console.error('Flight API error:', error);
    
    // Handle specific Notion API errors
    if (error.code === 'object_not_found') {
      return res.status(404).json({ error: 'Flight not found' });
    }
    
    res.status(500).json({ error: 'Internal server error' });
  }
});

// FlightAware real-time status endpoint
app.get('/api/flight/:flightId/status', async (req, res) => {
  try {
    const { flightId } = req.params;
    
    // Special case for test flight - return no data to hide the section
    if (flightId === 'test') {
      return res.json({
        status: 'No Data',
        message: 'Test flight - no real-time data available',
        lastUpdated: new Date().toISOString(),
        source: 'test'
      });
    }
    
    // Check if this is a flight ident (like "AS271") or a Notion page ID
    if (flightId.includes('-') && flightId.split('-').length >= 2) {
      // This is a Notion page ID format: {notionPageId}-{direction}
      const parts = flightId.split('-');
      const direction = parts.pop();
      let notionPageId = parts.join('-');
      
      // Convert 32-character page ID to UUID format if needed
      if (notionPageId.length === 32 && !notionPageId.includes('-')) {
        notionPageId = notionPageId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
      }
      
      if (!['departure', 'return'].includes(direction)) {
        return res.status(400).json({ error: 'Invalid direction. Must be "departure" or "return"' });
      }
    } else {
      // This is a flight ident (like "AS271") - we can't get Notion data, so return early
      return res.json({
        status: 'No Data',
        message: 'Flight ident provided - cannot fetch Notion data',
        lastUpdated: new Date().toISOString(),
        source: 'flight_ident'
      });
    }
    
    // Get basic flight data from Notion first
    const page = await notion.pages.retrieve({ page_id: notionPageId });
    const properties = page.properties;
    
    let airline, flightNumber, departureDate;
    
    if (direction === 'departure') {
      airline = properties.departure_airline?.select?.name;
      flightNumber = properties.departure_flightnumber?.title?.[0]?.text?.content;
      departureDate = properties.departure_time?.date?.start;
    } else {
      airline = properties.return_airline?.select?.name || properties.departure_airline?.select?.name;
      flightNumber = properties.return_flightnumber?.title?.[0]?.text?.content;
      departureDate = properties.return_time?.date?.start;
    }
    
    if (!airline || !flightNumber || !departureDate) {
      return res.status(400).json({ error: 'Missing required flight information' });
    }
    
    // Check if we should fetch real-time data (within 24 hours of departure)
    const now = new Date();
    const depDate = new Date(departureDate);
    const hoursUntilDeparture = (depDate - now) / (1000 * 60 * 60);
    
    // Only fetch real-time data if within 24 hours of departure and not more than 2 hours past arrival
    if (hoursUntilDeparture > 24 || hoursUntilDeparture < -2) {
      return res.json({
        status: 'Scheduled',
        message: 'Real-time tracking not available for this flight',
        lastUpdated: new Date().toISOString(),
        source: 'notion'
      });
    }
    
    // Create cache key
    const cacheKey = `flight-status:${airline}:${flightNumber}:${departureDate.split('T')[0]}`;
    
    // Check cache first
    if (cacheEnabled && redis) {
      try {
        const cached = await redis.get(cacheKey);
        if (cached) {
          const cachedData = JSON.parse(cached);
          console.log(`✅ Flight status cache hit for ${airline}${flightNumber}`);
          return res.json({
            ...cachedData,
            source: 'cache'
          });
        }
      } catch (cacheError) {
        console.warn('Cache read error:', cacheError.message);
      }
    }
    
    // Fetch from FlightAware API
    console.log(`🔄 Fetching real-time status for ${airline}${flightNumber}`);
    const flightStatus = await fetchFlightStatus(airline, flightNumber, departureDate);
    
    if (!flightStatus) {
      return res.json({
        status: 'No Data',
        message: 'Flight status not available from FlightAware',
        lastUpdated: new Date().toISOString(),
        source: 'notion'
      });
    }
    
    // Format the response
    const statusData = {
      status: flightStatus.status || 'Unknown',
      departureGate: flightStatus.origin_gate || null,
      departureTerminal: flightStatus.origin_terminal || null,
      arrivalGate: flightStatus.destination_gate || null,
      arrivalTerminal: flightStatus.destination_terminal || null,
      baggageClaim: flightStatus.baggage_claim || null,
      delay: flightStatus.delay || 0,
      estimatedDeparture: flightStatus.estimated_out || flightStatus.scheduled_out,
      estimatedArrival: flightStatus.estimated_in || flightStatus.scheduled_in,
      actualDeparture: flightStatus.actual_out,
      actualArrival: flightStatus.actual_in,
      lastUpdated: new Date().toISOString(),
      source: 'flightaware'
    };
    
    // Cache the result for 1 hour (3600 seconds)
    if (cacheEnabled && redis) {
      try {
        await redis.setex(cacheKey, 3600, JSON.stringify(statusData));
        console.log(`💾 Cached flight status for ${airline}${flightNumber}`);
      } catch (cacheError) {
        console.warn('Cache write error:', cacheError.message);
      }
    }
    
    res.json(statusData);
    
  } catch (error) {
    console.error('Flight status API error:', error);
    
    // Handle specific errors
    if (error.message === 'FlightAware API key not configured') {
      return res.status(503).json({ 
        error: 'Flight tracking service not configured',
        message: 'Real-time flight status is not available'
      });
    }
    
    if (error.response?.status === 401) {
      return res.status(503).json({ 
        error: 'Flight tracking service authentication failed',
        message: 'Real-time flight status is not available'
      });
    }
    
    if (error.response?.status === 429) {
      return res.status(503).json({ 
        error: 'Flight tracking service rate limit exceeded',
        message: 'Please try again later'
      });
    }
    
    // For other errors, return a fallback response
    res.json({
      status: 'Error',
      message: 'Unable to fetch real-time status',
      lastUpdated: new Date().toISOString(),
      source: 'error'
    });
  }
});

// Cache management endpoint - clear cache for a specific person
app.get('/cache/clear/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }
    
    // Clear all personal calendar cache variants
    const icsKey = `calendar:${personId}:ics`;
    const googleIcsKey = `calendar:${personId}:google_ics`;
    const jsonKey = `calendar:${personId}:json`;
    
    const icsDeleted = await redis.del(icsKey);
    const googleIcsDeleted = await redis.del(googleIcsKey);
    const jsonDeleted = await redis.del(jsonKey);
    
    res.json({
      success: true,
      message: 'Cache cleared successfully',
      personId: personId,
      cleared: {
        ics: icsDeleted > 0,
        google_ics: googleIcsDeleted > 0,
        json: jsonDeleted > 0
      }
    });
  } catch (error) {
    console.error('Cache clear error:', error);
    res.status(500).json({ error: 'Error clearing cache' });
  }
});

// Cache management endpoint - clear all caches
app.get('/cache/clear-all', async (req, res) => {
  try {
    // Get all calendar cache keys
    const keys = await redis.keys('calendar:*');
    
    if (keys.length === 0) {
      return res.json({
        success: true,
        message: 'No cache entries found',
        cleared: 0
      });
    }
    
    // Delete all cache keys
    const deleted = await redis.del(keys);
    
    res.json({
      success: true,
      message: 'All caches cleared successfully',
      cleared: deleted
    });
  } catch (error) {
    console.error('Cache clear all error:', error);
    res.status(500).json({ error: 'Error clearing all caches' });
  }
});

// Debug endpoint to explore Calendar Data database
app.get('/debug/calendar-data/:personId', async (req, res) => {
  try {
    let { personId } = req.params;

    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    if (!CALENDAR_DATA_DB) {
      return res.status(500).json({ error: 'CALENDAR_DATA_DATABASE_ID not configured' });
    }

    // Query Calendar Data database for events related to this person
    const response = await notion.databases.query({
      database_id: CALENDAR_DATA_DB,
      filter: {
        property: 'Personnel',
        relation: {
          contains: personId
        }
      }
    });

    // Return structured data for inspection
    const events = response.results.map(page => {
      const props = page.properties;
      return {
        id: page.id,
        name: props.Name?.title?.[0]?.text?.content || 'No name',
        personnel: props.Personnel?.relation || [],
        url: props.URL?.url || 'No URL',
        // Show the key event properties we need
        events: props.Events?.formula?.string || props.Events?.rich_text?.[0]?.text?.content || 'No events',
        flights: props.Flights?.formula?.string || props.Flights?.rich_text?.[0]?.text?.content || 'No flights',
        rehearsals: props.Rehearsals?.formula?.string || props.Rehearsals?.rich_text?.[0]?.text?.content || 'No rehearsals',
        transportation: props.Transportation?.formula?.string || props.Transportation?.rich_text?.[0]?.text?.content || 'No transportation',
        hotels: props.Hotels?.formula?.string || props.Hotels?.rich_text?.[0]?.text?.content || 'No hotels',
        teamCalendar: props['Team Calendar']?.formula?.string || props['Team Calendar']?.rich_text?.[0]?.text?.content || 'No team calendar',
        eventNotesReminders: props['Event Notes Reminders']?.formula?.string || props['Event Notes Reminders']?.rich_text?.[0]?.text?.content || 'No event notes reminders',
        // Show all available properties for debugging
        allProperties: Object.keys(props)
      };
    });
    
    // Count actual events from all JSON arrays and extract calltime info
    let totalActualEvents = 0;
    let calltimeDebug = [];
    
    events.forEach(event => {
      try {
        const eventsArray = JSON.parse(event.events || '[]');
        const flightsArray = JSON.parse(event.flights || '[]');
        const rehearsalsArray = JSON.parse(event.rehearsals || '[]');
        const transportationArray = JSON.parse(event.transportation || '[]');
        const hotelsArray = JSON.parse(event.hotels || '[]');
        const teamCalendarArray = JSON.parse(event.teamCalendar || '[]');
        const eventNotesRemindersArray = JSON.parse(event.eventNotesReminders || '[]');
        
        totalActualEvents += eventsArray.length + flightsArray.length + rehearsalsArray.length + 
                           transportationArray.length + hotelsArray.length + teamCalendarArray.length +
                           eventNotesRemindersArray.length;
        
        // DEBUG: Extract calltime info for specific events
        eventsArray.forEach(evt => {
          if (evt.event_name && (evt.event_name.includes('Pacific Palisades') || evt.event_name.includes('11-15') || (evt.calltime && evt.calltime.includes('2025-11-15')))) {
            // Extract raw calltime from formula string before parsing - find the event by name and extract its calltime
            const escapedEventName = evt.event_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const eventMatch = (event.events || '').match(new RegExp(`"event_name":"[^"]*${escapedEventName.substring(0, 10)}[^"]*"[^}]*"calltime":"([^"]*)"`));
            calltimeDebug.push({
              event_name: evt.event_name,
              raw_calltime_from_formula_string: eventMatch ? eventMatch[1] : null,
              raw_calltime_from_parsed_json: evt.calltime,
              raw_event_date_from_formula: evt.event_date,
              calltime_type: typeof evt.calltime,
              calltime_length: evt.calltime ? evt.calltime.length : 0,
              full_event: evt
            });
          }
        });
      } catch (e) {
        console.warn('Error parsing JSON in debug endpoint:', e);
      }
    });
    
    res.json({
      personId: personId,
      totalDatabaseRows: response.results.length,
      totalActualEvents: totalActualEvents,
      calltimeDebug: calltimeDebug,
      events: events
    });
  } catch (error) {
    console.error('Calendar Data debug error:', error);
    res.status(500).json({ error: 'Error querying Calendar Data', details: error.message });
  }
});

// Temporary diagnostic endpoint to verify parseUnifiedDateTime on the server
app.get('/debug/parse-test', async (req, res) => {
  try {
    const input = req.query.input || '2026-02-21T07:00:00-08:00/2026-02-22T00:00:00-08:00';
    const personId = req.query.person;
    const eventFilter = req.query.event;
    const result = parseUnifiedDateTime(input);
    const response = {
      input,
      startISO: result?.start?.toISOString?.() || null,
      endISO: result?.end?.toISOString?.() || null,
      startUTCHours: result?.start?.getUTCHours?.() ?? null,
      serverTZ: Intl.DateTimeFormat().resolvedOptions().timeZone,
      parserMode: 'no_conversion_wall_clock',
      version: 'no-conversion-v1'
    };
    if (personId) {
      const calendarData = await getCalendarDataFromDatabase(personId);
      if (calendarData?.events) {
        let evts = calendarData.events;
        if (eventFilter) {
          evts = evts.filter(e => e.event_name && e.event_name.toLowerCase().includes(eventFilter.toLowerCase()));
        }
        response.sampleEvents = evts.slice(0, 10).map(e => {
          const parsed = parseUnifiedDateTime(e.event_date);
          const ctParsed = e.calltime ? parseUnifiedDateTime(e.calltime, { faceValue: true }) : null;
          const ctDefault = e.calltime ? parseUnifiedDateTime(e.calltime) : null;
          const ctSmart = parseCalltimeSmart(e.calltime);
          const resolvedMainTimes = resolveMainEventTimes(e.event_date, e.calltime);
          return {
            name: e.event_name,
            raw_event_date: e.event_date,
            raw_calltime: e.calltime,
            parsed_event_start_utc: parsed?.start?.toISOString?.() || null,
            parsed_event_end_utc: parsed?.end?.toISOString?.() || null,
            parsed_event_start_hours: parsed?.start?.getUTCHours?.() ?? null,
            parsed_event_end_hours: parsed?.end?.getUTCHours?.() ?? null,
            calltime_faceValue_utc: ctParsed?.start?.toISOString?.() || null,
            calltime_faceValue_hours: ctParsed?.start?.getUTCHours?.() ?? null,
            calltime_utcConvert_utc: ctDefault?.start?.toISOString?.() || null,
            calltime_utcConvert_hours: ctDefault?.start?.getUTCHours?.() ?? null,
            calltime_smart_utc: ctSmart?.start?.toISOString?.() || null,
            calltime_smart_hours: ctSmart?.start?.getUTCHours?.() ?? null,
            end_compat_applied: resolvedMainTimes.endCompatApplied,
            end_compat_reason: resolvedMainTimes.endCompatReason,
            final_start_would_be: resolvedMainTimes.eventTimes?.start?.getUTCHours?.() ?? null,
            final_end_would_be: resolvedMainTimes.eventTimes?.end?.getUTCHours?.() ?? null,
          };
        });
      }
    }
    res.json(response);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/debug/admin-parse', async (req, res) => {
  try {
    const limitParam = parseInt(req.query.limit, 10);
    const limit = Number.isFinite(limitParam) ? Math.max(1, Math.min(limitParam, 200)) : 50;
    const contains = typeof req.query.contains === 'string' ? req.query.contains.toLowerCase() : null;
    const notionUrl = typeof req.query.url === 'string' ? req.query.url.trim() : null;

    const adminEvents = await getAdminCalendarData();

    let filtered = adminEvents;
    if (contains) {
      filtered = filtered.filter(event =>
        typeof event.event_name === 'string' &&
        event.event_name.toLowerCase().includes(contains)
      );
    }
    if (notionUrl) {
      filtered = filtered.filter(event => event.notion_url === notionUrl);
    }

    const sample = filtered.slice(0, limit).map(event => {
      const rawEventDate = event.event_date;
      const strictAtParse = parseAtHumanRangeNoConversion(rawEventDate, { includeMeta: true });
      const strictUnified = parseUnifiedDateTime(rawEventDate, {
        humanWallClock: true,
        atHumanNoConversion: true
      });
      const defaultUnified = parseUnifiedDateTime(rawEventDate);
      const strictResolved = resolveMainEventTimes(rawEventDate, event.calltime, {
        humanWallClock: true,
        atHumanNoConversion: true
      });

      return {
        title: event.event_name || '',
        notion_url: event.notion_url || '',
        raw_event_date: rawEventDate || '',
        raw_calltime: event.calltime || '',
        has_at_prefix: typeof rawEventDate === 'string' && rawEventDate.trim().startsWith('@'),
        strict_at_branch: strictAtParse?.__branch || null,
        strict_at_start_iso: strictAtParse?.start?.toISOString?.() || null,
        strict_at_end_iso: strictAtParse?.end?.toISOString?.() || null,
        unified_strict_start_iso: strictUnified?.start?.toISOString?.() || null,
        unified_strict_end_iso: strictUnified?.end?.toISOString?.() || null,
        unified_default_start_iso: defaultUnified?.start?.toISOString?.() || null,
        unified_default_end_iso: defaultUnified?.end?.toISOString?.() || null,
        resolved_strict_start_iso: strictResolved?.eventTimes?.start?.toISOString?.() || null,
        resolved_strict_end_iso: strictResolved?.eventTimes?.end?.toISOString?.() || null,
        resolved_strict_end_compat_applied: strictResolved?.endCompatApplied || false,
        resolved_strict_end_compat_reason: strictResolved?.endCompatReason || null
      };
    });

    res.json({
      success: true,
      total_admin_events: adminEvents.length,
      filtered_events: filtered.length,
      returned: sample.length,
      serverTZ: Intl.DateTimeFormat().resolvedOptions().timeZone,
      parserMode: 'no_conversion_wall_clock',
      parserVersion: 'admin-at-no-conversion-debug-v1',
      filters: {
        contains: contains || null,
        url: notionUrl || null,
        limit
      },
      events: sample
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Regeneration endpoint - regenerate calendar for a specific person
app.get('/regenerate/:personId', async (req, res) => {
  let manualRegenRegistered = false;
  try {
    let { personId } = req.params;
    const regenMode = parseRegenMode(req.query.mode ?? req.query.regenMode ?? req.query.regen_mode);
    if (!regenMode) {
      return res.status(400).json({
        success: false,
        error: 'Invalid regen mode',
        message: 'Use mode=full | mode=events_only | mode=non_events_only'
      });
    }
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    if (!isSplitModeAllowedForPerson(personId, regenMode)) {
      return res.status(400).json({
        success: false,
        error: 'Regen mode not allowed for this person',
        message: `Split modes are only enabled for test person ${SPLIT_REGEN_TEST_PERSON_ID}`,
        regenMode
      });
    }

    activeManualRegens += 1;
    manualRegenRegistered = true;

    const result = await regenerateCalendarForPerson(personId, {
      trigger: `manual_regen:${regenMode}`,
      clearCache: true,
      regenMode
    });
    if (result.success) {
      return res.json({
        success: true,
        message: 'Calendar regenerated successfully',
        personId,
        personName: result.personName,
        regen_mode: result.regenMode || regenMode,
        eventCount: result.eventCount,
        cache_cleared: true,
        cached_for_seconds: CACHE_TTL
      });
    }
    const statusCode = result.reason === 'no_events' ? 404 : 500;
    return res.status(statusCode).json({
      success: false,
      message: result.reason === 'no_events'
        ? 'No events found for this person'
        : 'Calendar regeneration failed',
      personId,
      regen_mode: regenMode,
      reason: result.reason || 'unknown',
      details: result.error || null
    });
  } catch (error) {
    console.error('Regeneration endpoint error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Error regenerating calendar',
      details: error.message
    });
  } finally {
    if (manualRegenRegistered) {
      activeManualRegens = Math.max(0, activeManualRegens - 1);
    }
  }
});

app.get('/calendar-data/regenerate/:pageId', async (req, res) => {
  const encodedPageId = encodeURIComponent(req.params.pageId);
  return res.redirect(307, `/calendar-data/regenerate?id=${encodedPageId}`);
});

app.get('/calendar-data/regenerate', async (req, res) => {
  let manualRegenRegistered = false;
  try {
    const calendarDataInput = req.query.id || req.query.url || req.query.pageId;
    const regenMode = parseRegenMode(req.query.mode ?? req.query.regenMode ?? req.query.regen_mode);
    if (!regenMode) {
      return res.status(400).json({
        success: false,
        error: 'Invalid regen mode',
        message: 'Use mode=full | mode=events_only | mode=non_events_only'
      });
    }
    if (!calendarDataInput || typeof calendarDataInput !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'Calendar Data page ID or URL is required',
        message: 'Provide ?id=<page-id> or ?url=<notion-url>'
      });
    }

    activeManualRegens += 1;
    manualRegenRegistered = true;

    const fetchTimeoutMs = REGEN_FETCH_STEP_TIMEOUT_MS;
    const { pageId, linkedPersonId, calendarData, source } = await withTimeout(
      regenMode === REGEN_MODE_EVENTS_ONLY
        ? getCalendarDataEventsOnlyFromPageIdOrUrl(calendarDataInput, 6)
        : getCalendarDataFromPageIdOrUrl(calendarDataInput, 6),
      fetchTimeoutMs,
      'Calendar Data page fetch timed out'
    );

    if (!linkedPersonId) {
      return res.status(400).json({
        success: false,
        error: 'Calendar Data row is not linked to a Personnel record',
        calendarDataPageId: pageId
      });
    }
    if (!isSplitModeAllowedForPerson(linkedPersonId, regenMode)) {
      return res.status(400).json({
        success: false,
        error: 'Regen mode not allowed for this person',
        message: `Split modes are only enabled for test person ${SPLIT_REGEN_TEST_PERSON_ID}`,
        calendarDataPageId: pageId,
        personId: linkedPersonId,
        regenMode
      });
    }

    const result = await regenerateCalendarForPerson(linkedPersonId, {
      trigger: `manual_regen_calendar_data:${regenMode}`,
      clearCache: true,
      preloadedCalendarData: calendarData,
      regenMode
    });

    if (result.success) {
      return res.json({
        success: true,
        message: 'Calendar regenerated successfully',
        personId: linkedPersonId,
        personName: result.personName,
        regen_mode: result.regenMode || regenMode,
        eventCount: result.eventCount,
        calendarDataPageId: pageId,
        calendarDataSource: source,
        cache_cleared: true,
        cached_for_seconds: CACHE_TTL
      });
    }

    const statusCode = result.reason === 'no_events' ? 404 : 500;
    return res.status(statusCode).json({
      success: false,
      message: result.reason === 'no_events'
        ? 'No events found for this Calendar Data record'
        : 'Calendar regeneration failed',
      personId: linkedPersonId,
      regen_mode: regenMode,
      calendarDataPageId: pageId,
      reason: result.reason || 'unknown',
      details: result.error || null
    });
  } catch (error) {
    console.error('Calendar Data regeneration endpoint error:', error);
    res.status(500).json({
      success: false,
      error: 'Error regenerating calendar from Calendar Data',
      details: error.message
    });
  } finally {
    if (manualRegenRegistered) {
      activeManualRegens = Math.max(0, activeManualRegens - 1);
    }
  }
});

app.get('/regenerate/:personId/status', async (req, res) => {
  res.status(410).json({
    success: false,
    error: 'Regeneration status endpoint retired',
    message: 'Personnel regeneration now runs synchronously. Call /regenerate/:personId directly instead.'
  });
});

// Regeneration endpoint - regenerate all calendars
app.get('/regenerate-all', async (req, res) => {
  try {
    // Start the regeneration process
    res.json({
      success: true,
      message: 'Bulk calendar regeneration started',
      note: 'This will take several minutes. Check server logs for progress.'
    });
    
    // Run regeneration in the background (don't await)
    regenerateAllCalendars().then(result => {
      console.log('Bulk regeneration completed:', result);
    }).catch(error => {
      console.error('Bulk regeneration failed:', error);
    });
    
  } catch (error) {
    console.error('Regeneration all endpoint error:', error);
    res.status(500).json({ 
      success: false,
      error: 'Error starting bulk regeneration',
      details: error.message
    });
  }
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
      calendarFeedLength: person.properties?.['Calendar Feed JSON']?.formula?.string?.length || 0,
      calendarFeedPreview: person.properties?.['Calendar Feed JSON']?.formula?.string?.substring(0, 200) || 'null',
      // Also test the "Test Calendar Feed" property
      testCalendarFeedExists: !!person.properties?.['Test Calendar Feed'],
      testCalendarFeedLength: person.properties?.['Test Calendar Feed']?.formula?.string?.length || 0,
      testCalendarFeedPreview: person.properties?.['Test Calendar Feed']?.formula?.string?.substring(0, 200) || 'null'
    };

    // Debug logging removed for performance

    res.json(testResults);
  } catch (error) {
    console.error('Simple test error:', error);
    res.status(500).json({ error: 'Error in simple test', details: error.message });
  }
});

// Calendar subscription endpoint with proper headers
// Admin calendar subscription page
app.get('/subscribe/admin', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL like /subscribe/admin%20%20...)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/admin' && originalPath.startsWith('/subscribe/admin')) {
    return res.redirect(301, '/subscribe/admin');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/admin`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/admin.ics');
    }
    
    // For web browsers, show a subscription page with same styling as personal calendars
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as personal calendars but with admin-specific content
    const adminSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Admin Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .copy-btn.copied {
            background: #2ecc71;
            color: #000;
            border-color: #2ecc71;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Admin Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all upcoming events across all personnel in your calendar app. Includes event details, venues, personnel, general info, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/admin" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible">
                <div class="collapsible-header" onclick="toggleCollapsible()">
                    Other Calendar Apps (Outlook, etc.)
                </div>
                <div class="collapsible-content" id="collapsibleContent">
                    <div class="collapsible-inner">
                        <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                        <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                        <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                        <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                            <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                            <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const header = event.currentTarget;
            const content = document.getElementById('collapsibleContent');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `;
    
    res.send(adminSubscriptionPage);
  } catch (error) {
    console.error('Error loading admin subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

app.get('/subscribe/travel', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL like /subscribe/travel%20%20...)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/travel' && originalPath.startsWith('/subscribe/travel')) {
    return res.redirect(301, '/subscribe/travel');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/travel`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/travel.ics');
    }
    
    // For web browsers, show a subscription page with same styling as personal calendars
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as personal calendars but with travel-specific content
    const travelSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Travel Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
            cursor: pointer;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .copy-btn.copied {
            background: #2ecc71;
            color: #000;
            border-color: #2ecc71;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        .collapsible {
            margin-top: 20px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
            border-color: #333;
        }
        
        .collapsible-header::after {
            content: '▼';
            font-size: 0.7rem;
            transition: transform 0.3s ease;
        }
        
        .collapsible-header.active::after {
            transform: rotate(180deg);
        }
        
        .collapsible-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }
        
        .collapsible-content.active {
            max-height: 500px;
        }
        
        .collapsible-inner {
            background: #141414;
            border: 1px solid #2a2a2a;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 24px;
            margin-top: -8px;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Travel Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all travel events across all personnel in your calendar app. Includes flight details, hotel information, travel dates, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/travel" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible">
                <div class="collapsible-header" onclick="toggleCollapsible()">
                    Other Calendar Apps (Outlook, etc.)
                </div>
                <div class="collapsible-content" id="collapsibleContent">
                    <div class="collapsible-inner">
                        <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                        <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                        <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                        <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                            <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                            <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const header = event.currentTarget;
            const content = document.getElementById('collapsibleContent');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `;
    
    res.send(travelSubscriptionPage);
  } catch (error) {
    console.error('Error loading travel subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

// Blockout calendar subscription page
app.get('/subscribe/blockout', async (req, res) => {
  // Redirect if URL has extra characters (malformed URL)
  const originalPath = decodeURIComponent(req.originalUrl.split('?')[0]);
  if (originalPath !== '/subscribe/blockout' && originalPath.startsWith('/subscribe/blockout')) {
    return res.redirect(301, '/subscribe/blockout');
  }
  try {
    const subscriptionUrl = `https://${req.get('host')}/calendar/blockout`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, '/calendar/blockout.ics');
    }
    
    // For web browsers, show a subscription page
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    
    // Use the same template as travel calendar but with blockout-specific content
    const blockoutSubscriptionPage = `
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Blockout Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            width: 100%;
            font-weight: 500;
        }
        
        .copy-btn:hover { 
            background: #222; 
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Subscribe to Blockout Calendar</h1>
            <div class="separator"></div>
            <div class="description">View all blockout events in your calendar app. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/blockout" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="calendar-card">
            <div class="collapsible-header" onclick="toggleCollapsible()">
                Other Calendar Apps (Outlook, etc.)
            </div>
            <div class="collapsible-content" id="collapsibleContent" style="max-height: 0; overflow: hidden; transition: max-height 0.3s ease;">
                <div style="padding: 24px; background: #0a0a0a; border: 1px solid #2a2a2a; border-top: none; border-radius: 0 0 8px 8px; margin-top: -8px;">
                    <p style="margin: 0 0 16px 0; color: #999; font-size: 0.9rem;">Copy this URL and add it to your calendar app:</p>
                    <div class="url-box" onclick="copyUrl()">${subscriptionUrl}</div>
                    <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                    <p style="margin: 16px 0 0 0; color: #666; font-size: 0.85rem; line-height: 1.6;">
                        <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br>
                        <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                    </p>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function copyAndOpenGoogle() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/r/settings/addbyurl', '_blank');
                }, 300);
            });
        }
        
        function copyUrl() {
            const url = '${subscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
            });
        }
        
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2000);
        }
        
        function toggleCollapsible() {
            const content = document.getElementById('collapsibleContent');
            if (content.style.maxHeight === '0px' || !content.style.maxHeight) {
                content.style.maxHeight = '500px';
            } else {
                content.style.maxHeight = '0px';
            }
        }
    </script>
</body>
</html>
    `;
    
    res.send(blockoutSubscriptionPage);
  } catch (error) {
    console.error('Error loading blockout subscription page:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

app.get('/subscribe/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Get person name from URL query parameter only
    const personName = req.query.name || null;
    
    const subscriptionUrl = `https://${req.get('host')}/calendar/${personId}.ics`;
    const googleSubscriptionUrl = `https://${req.get('host')}/calendar/google/${personId}.ics`;
    
    // Check if this is a calendar app request
    const userAgent = req.headers['user-agent'] || '';
    const isCalendarApp = userAgent.toLowerCase().includes('calendar') || 
                         userAgent.toLowerCase().includes('caldav') ||
                         req.headers.accept?.includes('text/calendar');
    
    if (isCalendarApp) {
      // Redirect calendar apps directly to the calendar feed
      return res.redirect(302, `/calendar/${personId}`);
    }
    
    // For web browsers, show a subscription page with instructions
    res.setHeader('Content-Type', 'text/html');
    res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Subscribe to Downbeat Calendar</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {
            box-sizing: border-box;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 40px 20px; 
            background: #000000; 
            color: #e0e0e0; 
            min-height: 100vh;
            line-height: 1.6;
        }
        
        .container { 
            max-width: 560px; 
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        h1 { 
            color: #fff; 
            margin: 0 0 12px 0; 
            font-size: 2.2rem; 
            font-weight: 500;
            letter-spacing: 0.5px;
        }
        
        .subtitle {
            color: #888;
            font-size: 1rem;
            font-weight: 400;
            margin: 0;
        }
        
        .separator {
            width: 100px;
            height: 1px;
            background: #2a2a2a;
            margin: 16px auto;
        }
        
        .description {
            color: #999;
            font-size: 0.95rem;
            font-weight: 400;
            text-align: center;
            margin: 24px auto 40px auto;
            max-width: 480px;
            line-height: 1.5;
        }
        
        .description strong {
            color: #bbb;
            font-weight: 600;
        }
        
        .calendar-card {
            background: #141414;
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .calendar-card.primary {
            border: 2px solid #2c2c2c;
        }
        
        .calendar-card.primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.5);
        }
        
        .calendar-button {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
            padding: 20px 32px;
            background: #1a1a1a;
            border: 2px solid #333;
            border-radius: 10px;
            color: #fff;
            text-decoration: none;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            cursor: pointer;
            width: 100%;
            position: relative;
        }
        
        .calendar-button:hover {
            background: #222;
            border-color: #444;
            transform: translateY(-1px);
        }
        
        .calendar-button:active {
            transform: translateY(0);
        }
        
        .calendar-button.primary {
            background: linear-gradient(135deg, #1a1a1a 0%, #2a2a2a 100%);
            border-color: #4a4a4a;
        }
        
        .calendar-button img {
            width: 36px;
            height: 36px;
            object-fit: contain;
        }
        
        .calendar-button.primary img {
            filter: brightness(0) invert(1);
        }
        
        .badge {
            position: absolute;
            top: -8px;
            right: 16px;
            background: #2ecc71;
            color: #000;
            font-size: 0.7rem;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .steps {
            margin-top: 24px;
            padding-top: 24px;
            border-top: 1px solid #2a2a2a;
        }
        
        .step {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            align-items: start;
        }
        
        .step:last-child {
            margin-bottom: 0;
        }
        
        .step-number {
            flex-shrink: 0;
            width: 28px;
            height: 28px;
            background: #2a2a2a;
            color: #fff;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 0.9rem;
        }
        
        .step-text {
            color: #b0b0b0;
            font-size: 0.95rem;
            padding-top: 4px;
        }
        
        .step-text strong {
            color: #e0e0e0;
        }
        
        .collapsible {
            margin-top: 20px;
        }
        
        .collapsible-header {
            background: transparent;
            border: 1px solid #2a2a2a;
            border-radius: 8px;
            padding: 16px 20px;
            color: #888;
            cursor: pointer;
            text-align: center;
            font-size: 0.9rem;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }
        
        .collapsible-header:hover {
            background: #141414;
            color: #b0b0b0;
            border-color: #333;
        }
        
        .collapsible-header::after {
            content: '▼';
            font-size: 0.7rem;
            transition: transform 0.3s ease;
        }
        
        .collapsible-header.active::after {
            transform: rotate(180deg);
        }
        
        .collapsible-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }
        
        .collapsible-content.active {
            max-height: 500px;
        }
        
        .collapsible-inner {
            background: #141414;
            border: 1px solid #2a2a2a;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 24px;
            margin-top: -8px;
        }
        
        .url-box { 
            background: #0a0a0a; 
            padding: 16px; 
            border-radius: 6px; 
            border: 1px solid #2a2a2a; 
            margin: 16px 0; 
            word-break: break-all; 
            font-family: 'Monaco', 'Menlo', monospace;
            color: #888;
            font-size: 13px;
            line-height: 1.5;
        }
        
        .copy-btn { 
            background: #1a1a1a; 
            color: #fff; 
            border: 1px solid #333; 
            padding: 12px 24px; 
            border-radius: 6px; 
            cursor: pointer; 
            font-size: 0.95rem;
            transition: all 0.3s ease;
            width: 100%;
            font-weight: 500;
        }
        
        .copy-btn:hover { 
            background: #222; 
            border-color: #444;
        }
        
        .divider {
            color: #444;
            font-size: 0.85rem;
            text-align: center;
            margin: 16px 0;
        }
        
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(100px);
            background: #2ecc71;
            color: #000;
            padding: 14px 28px;
            border-radius: 8px;
            font-weight: 500;
            font-size: 0.95rem;
            box-shadow: 0 4px 20px rgba(46, 204, 113, 0.4);
            opacity: 0;
            transition: all 0.3s ease;
            z-index: 1000;
        }
        
        .toast.show {
            transform: translateX(-50%) translateY(0);
            opacity: 1;
        }
        
        @media (max-width: 600px) {
            body {
                padding: 20px 16px;
            }
            
            .calendar-card {
                padding: 24px 20px;
            }
            
            h1 {
                font-size: 1.8rem;
            }
            
            .calendar-button {
                padding: 18px 24px;
                font-size: 1rem;
            }
            
            .badge {
                font-size: 0.65rem;
                padding: 3px 8px;
                right: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>${personName ? `Hello ${personName}!` : 'Subscribe to Calendar'}</h1>
            <div class="separator"></div>
            <div class="description"><strong>Introducing Downbeat Calendar</strong> - your personalized event calendar with everything you need: call times, venue details, MD contacts, payroll info, flights, hotels, and more. Subscribe once and stay organized across all your devices.</div>
        </div>
        
        <!-- Apple Calendar - Primary -->
        <div class="calendar-card primary">
            <a href="webcal://${req.get('host')}/calendar/${personId}" class="calendar-button primary">
                <img src="/Apple%20Logo.png" alt="Apple" onerror="this.style.display='none'">
                <span>Subscribe with Apple Calendar</span>
                <span class="badge">One Click</span>
            </a>
        </div>
        
        <!-- Google Calendar - Secondary -->
        <div class="calendar-card">
            <button class="calendar-button" onclick="copyAndOpenGoogle()">
                <img src="/Google%20Logo.png" alt="Google" onerror="this.style.display='none'">
                <span>Subscribe with Google Calendar</span>
            </button>
            
            <div class="steps">
                <div class="step">
                    <div class="step-number">1</div>
                    <div class="step-text">Click the button above to <strong>copy the URL</strong> and open Google Calendar</div>
                </div>
                <div class="step">
                    <div class="step-number">2</div>
                    <div class="step-text">Select <strong>"From URL"</strong> in the left menu</div>
                </div>
                <div class="step">
                    <div class="step-number">3</div>
                    <div class="step-text">Paste the URL and click <strong>"Add calendar"</strong></div>
                </div>
            </div>
        </div>
        
        <!-- Other Apps - Collapsible -->
        <div class="collapsible">
            <div class="collapsible-header" onclick="toggleCollapsible()">
                Other Calendar Apps (Outlook, etc.)
            </div>
            <div class="collapsible-content">
                <div class="collapsible-inner">
                    <p style="margin: 0 0 16px 0; color: #888; font-size: 0.9rem;">
                        Copy this URL and add it to your calendar app:
                    </p>
                    <div class="url-box" id="urlBox">${subscriptionUrl}</div>
                    <button class="copy-btn" onclick="copyUrl()">Copy URL</button>
                    <div class="divider">• • •</div>
                    <div class="step-text" style="margin-top: 16px;">
                        <strong>Outlook:</strong> Calendar → Add calendar → Subscribe from web → Paste URL<br><br>
                        <strong>Other apps:</strong> Look for "Subscribe to calendar" or "Add calendar from URL" option
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <div class="toast" id="toast">✓ URL copied to clipboard!</div>
    
    <script>
        function showToast() {
            const toast = document.getElementById('toast');
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
            }, 2500);
        }
        
        function copyUrl() {
            const urlBox = document.getElementById('urlBox');
            navigator.clipboard.writeText(urlBox.textContent).then(() => {
                showToast();
            });
        }
        
        function copyAndOpenGoogle() {
            const url = '${googleSubscriptionUrl}';
            navigator.clipboard.writeText(url).then(() => {
                showToast();
                // Small delay so user sees the toast before opening new tab
                setTimeout(() => {
                    window.open('https://calendar.google.com/calendar/u/0/r/settings/addcalendar', '_blank', 'noopener,noreferrer');
                }, 300);
            });
        }
        
        function toggleCollapsible() {
            const header = document.querySelector('.collapsible-header');
            const content = document.querySelector('.collapsible-content');
            header.classList.toggle('active');
            content.classList.toggle('active');
        }
    </script>
</body>
</html>
    `);
    
  } catch (error) {
    console.error('Subscription page error:', error);
    res.status(500).json({ error: 'Error loading subscription page' });
  }
});

// ============================================
// ADMIN CALENDAR ENDPOINT
// ============================================
app.get('/admin/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:admin:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          verboseLog(`✅ Cache HIT for admin calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="admin-calendar.ics"');
            return res.send(cachedData);
          }
        }
        logWithDedup(
          `cache_miss:admin:${format.toLowerCase()}`,
          `❌ Cache MISS for admin calendar (${format.toUpperCase()})`
        );
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if admin calendar is configured
    if (!ADMIN_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Admin calendar not configured',
        message: 'ADMIN_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Admin Calendar',
          description: 'Admin calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch admin calendar data
    let adminEvents;
    try {
      adminEvents = await withTimeout(
        getAdminCalendarData(),
        CALENDAR_FETCH_TIMEOUT_MS,
        `Admin calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
      );
      
      if (!adminEvents || adminEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Admin Events property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Admin Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching admin calendar data:', error);
      const isTransientNotionFailure =
        error.message?.includes('504') ||
        error.message?.includes('timeout') ||
        error.message?.includes('Gateway Timeout') ||
        error.code === 'notion_circuit_open';

      if (isTransientNotionFailure && redis && cacheEnabled) {
        console.log('⚠️  Notion transient failure - attempting to return cached admin data...');
        try {
          const cachedData = await redis.get(cacheKey);
          if (cachedData) {
            console.log('✅ Returning cached admin calendar data');
            if (format === 'json') {
              res.setHeader('Content-Type', 'application/json');
              return res.send(cachedData);
            }
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="admin-calendar.ics"');
            return res.send(cachedData);
          }
        } catch (cacheError) {
          console.error('Error retrieving cached admin data:', cacheError);
        }
      }
      
      const errorMsg = {
        error: 'Error fetching admin calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        const errorCalendar = ical({ 
          name: 'Admin Calendar',
          description: `Error: ${error.message}`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processAdminEvents(adminEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Admin Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, jsonData);
          verboseLog(`💾 Cached admin calendar JSON (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Admin Calendar',
        description: 'All upcoming events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: []  // No alarms for admin calendar
        });
      });
      
      const icsData = serializeCalendar(calendar);
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, icsData);
          verboseLog(`💾 Cached admin calendar ICS (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="admin-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Admin calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating admin calendar',
      message: error.message
    });
  }
});

// Admin calendar regeneration endpoint (clears cache and regenerates)
app.get('/admin/calendar/regen', async (req, res) => {
  try {
    if (!ADMIN_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Admin calendar not configured',
        message: 'ADMIN_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating admin calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:admin:ics');
        await redis.del('calendar:admin:json');
        console.log('✅ Admin calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const adminEvents = await withTimeout(
      getAdminCalendarData(),
      CALENDAR_FETCH_TIMEOUT_MS,
      `Admin calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
    );
    const allCalendarEvents = processAdminEvents(adminEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Admin Calendar',
      description: 'All upcoming events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: []  // No alarms for admin calendar
      });
    });
    
    const icsData = serializeCalendar(calendar);
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Admin Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await setCalendarCache('calendar:admin:ics', icsData);
        await setCalendarCache('calendar:admin:json', jsonData);
        console.log(`💾 Admin calendar regenerated and cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    // Return success response
    res.json({
      success: true,
      message: 'Admin calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cache_cleared: true,
      cached_for_seconds: CACHE_TTL
    });

  } catch (error) {
    console.error('Admin calendar regen error:', error);
    res.status(500).json({ 
      error: 'Error regenerating admin calendar',
      message: error.message
    });
  }
});

// ============================================
// TRAVEL CALENDAR ENDPOINTS
// ============================================

app.get('/travel/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:travel:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          verboseLog(`✅ Cache HIT for travel calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
            return res.send(cachedData);
          }
        }
        logWithDedup(
          `cache_miss:travel:${format.toLowerCase()}`,
          `❌ Cache MISS for travel calendar (${format.toUpperCase()})`
        );
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if travel calendar is configured
    if (!TRAVEL_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Travel calendar not configured',
        message: 'TRAVEL_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Travel Calendar',
          description: 'Travel calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch travel calendar data
    let travelEvents;
    try {
      travelEvents = await withTimeout(
        getTravelCalendarData(),
        CALENDAR_FETCH_TIMEOUT_MS,
        `Travel calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
      );
      
      if (!travelEvents || travelEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Travel Admin property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Travel Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching travel calendar data:', error);
      
      // If Notion API times out, try to return cached data as fallback
      const isTimeout =
        error.message?.includes('504') ||
        error.message?.includes('timeout') ||
        error.message?.includes('Gateway Timeout') ||
        error.code === 'notion_circuit_open';
      
      if (isTimeout && redis && cacheEnabled) {
        console.log('⚠️  Notion API timeout - attempting to return cached data...');
        try {
          const cachedData = await redis.get(cacheKey);
          if (cachedData) {
            console.log(`✅ Returning cached travel calendar data (fallback from timeout)`);
            if (format === 'json') {
              res.setHeader('Content-Type', 'application/json');
              return res.send(cachedData);
            } else {
              res.setHeader('Content-Type', 'text/calendar');
              res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
              return res.send(cachedData);
            }
          }
        } catch (cacheError) {
          console.error('Error retrieving cached data:', cacheError);
        }
      }
      
      const errorMsg = {
        error: 'Error fetching travel calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Always return a valid ICS file, even on error
        const errorCalendar = ical({ 
          name: 'Travel Calendar',
          description: `Error: ${error.message}. Please try again later.`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processTravelEvents(travelEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Travel Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, jsonData);
          verboseLog(`💾 Cached travel calendar JSON (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Travel Calendar',
        description: 'All travel events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: []  // No alarms for travel calendar
        });
      });
      
      const icsData = serializeCalendar(calendar);
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, icsData);
          verboseLog(`💾 Cached travel calendar ICS (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="travel-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Travel calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating travel calendar',
      message: error.message
    });
  }
});

// Travel calendar regeneration endpoint (clears cache and regenerates)
app.get('/travel/calendar/regen', async (req, res) => {
  try {
    if (!TRAVEL_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Travel calendar not configured',
        message: 'TRAVEL_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating travel calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:travel:ics');
        await redis.del('calendar:travel:json');
        console.log('✅ Travel calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const travelEvents = await withTimeout(
      getTravelCalendarData(),
      CALENDAR_FETCH_TIMEOUT_MS,
      `Travel calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
    );
    const allCalendarEvents = processTravelEvents(travelEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Travel Calendar',
      description: 'All travel events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: []  // No alarms for travel calendar
      });
    });
    
    const icsData = serializeCalendar(calendar);
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Travel Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await setCalendarCache('calendar:travel:ics', icsData);
        await setCalendarCache('calendar:travel:json', jsonData);
        console.log(`💾 Travel calendar regenerated and cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    // Return success response
    res.json({
      success: true,
      message: 'Travel calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cache_cleared: true,
      cached_for_seconds: CACHE_TTL
    });

  } catch (error) {
    console.error('Travel calendar regen error:', error);
    res.status(500).json({ 
      error: 'Error regenerating travel calendar',
      message: error.message
    });
  }
});

// ============================================
// BLOCKOUT CALENDAR ENDPOINTS
// ============================================

app.get('/blockout/calendar', async (req, res) => {
  try {
    const format = req.query.format || (req.headers.accept?.includes('application/json') ? 'json' : 'ics');
    const forceFresh = req.query.fresh === 'true';
    const cacheKey = `calendar:blockout:${format}`;
    
    // Check cache first (unless fresh requested)
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          verboseLog(`✅ Cache HIT for blockout calendar (${format.toUpperCase()})`);
          
          if (format === 'json') {
            res.setHeader('Content-Type', 'application/json');
            return res.send(cachedData);
          } else {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
            return res.send(cachedData);
          }
        }
        logWithDedup(
          `cache_miss:blockout:${format.toLowerCase()}`,
          `❌ Cache MISS for blockout calendar (${format.toUpperCase()})`
        );
      } catch (cacheError) {
        console.error('Redis cache error:', cacheError);
      }
    }
    
    // Check if blockout calendar is configured
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      const errorMsg = { 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Return empty calendar for calendar apps
        const emptyCalendar = ical({ 
          name: 'Blockout Calendar',
          description: 'Blockout calendar not configured'
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(emptyCalendar.toString());
      }
    }
    
    // Fetch blockout calendar data
    let blockoutEvents;
    try {
      blockoutEvents = await withTimeout(
        getBlockoutCalendarData(),
        CALENDAR_FETCH_TIMEOUT_MS,
        `Blockout calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
      );
      
      if (!blockoutEvents || blockoutEvents.length === 0) {
        const noEventsMsg = {
          error: 'No events found',
          message: 'Blockout Admin property is empty or contains no events'
        };
        
        if (format === 'json') {
          return res.status(404).json(noEventsMsg);
        } else {
          const emptyCalendar = ical({ 
            name: 'Blockout Calendar',
            description: 'No events found'
          });
          res.setHeader('Content-Type', 'text/calendar');
          return res.send(emptyCalendar.toString());
        }
      }
    } catch (error) {
      console.error('Error fetching blockout calendar data:', error);
      
      // If Notion API times out, try to return cached data as fallback
      const isTimeout =
        error.message?.includes('504') ||
        error.message?.includes('timeout') ||
        error.message?.includes('Gateway Timeout') ||
        error.code === 'notion_circuit_open';
      
      if (isTimeout && redis && cacheEnabled) {
        console.log('⚠️  Notion API timeout - attempting to return cached data...');
        try {
          const cachedData = await redis.get(cacheKey);
          if (cachedData) {
            console.log(`✅ Returning cached blockout calendar data (fallback from timeout)`);
            if (format === 'json') {
              res.setHeader('Content-Type', 'application/json');
              return res.send(cachedData);
            } else {
              res.setHeader('Content-Type', 'text/calendar');
              res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
              return res.send(cachedData);
            }
          }
        } catch (cacheError) {
          console.error('Error retrieving cached data:', cacheError);
        }
      }
      
      const errorMsg = {
        error: 'Error fetching blockout calendar data',
        message: error.message
      };
      
      if (format === 'json') {
        return res.status(500).json(errorMsg);
      } else {
        // Always return a valid ICS file, even on error
        const errorCalendar = ical({ 
          name: 'Blockout Calendar',
          description: `Error: ${error.message}. Please try again later.`
        });
        res.setHeader('Content-Type', 'text/calendar');
        return res.send(errorCalendar.toString());
      }
    }
    
    // Process events
    const allCalendarEvents = processBlockoutEvents(blockoutEvents);
    
    // Return based on format
    if (format === 'json') {
      const jsonData = JSON.stringify({
        calendar_name: 'Blockout Calendar',
        total_events: allCalendarEvents.length,
        events: allCalendarEvents
      }, null, 2);
      
      // Cache the JSON
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, jsonData);
          verboseLog(`💾 Cached blockout calendar JSON (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'application/json');
      return res.send(jsonData);
    } else {
      // Generate ICS
      const calendar = ical({ 
        name: 'Blockout Calendar',
        description: 'All blockout events',
        ttl: 300
      });
      
      allCalendarEvents.forEach(event => {
        const startDate = event.start instanceof Date ? event.start : new Date(event.start);
        const endDate = event.end instanceof Date ? event.end : new Date(event.end);
        
        calendar.createEvent({
          start: startDate,
          end: endDate,
          summary: event.title,
          description: event.description,
          location: event.location,
          url: event.url || '',
          floating: true,
          alarms: getAlarmsForEvent(event.type, event.title)
        });
      });
      
      const icsData = serializeCalendar(calendar);
      
      // Cache the ICS
      if (redis && cacheEnabled) {
        try {
          await setCalendarCache(cacheKey, icsData);
          verboseLog(`💾 Cached blockout calendar ICS (${CACHE_TTL}s TTL)`);
        } catch (cacheError) {
          console.error('Redis cache write error:', cacheError);
        }
      }
      
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', 'attachment; filename="blockout-calendar.ics"');
      return res.send(icsData);
    }
    
  } catch (error) {
    console.error('Blockout calendar error:', error);
    res.status(500).json({ 
      error: 'Error generating blockout calendar',
      message: error.message
    });
  }
});

// Blockout calendar regeneration endpoint (clears cache and regenerates)
app.get('/blockout/calendar/regen', async (req, res) => {
  try {
    if (!BLOCKOUT_CALENDAR_PAGE_ID) {
      return res.status(500).json({ 
        error: 'Blockout calendar not configured',
        message: 'BLOCKOUT_CALENDAR_PAGE_ID environment variable not set'
      });
    }

    console.log('🔄 Regenerating blockout calendar (clearing cache)...');
    
    // Clear both ICS and JSON caches
    if (redis && cacheEnabled) {
      try {
        await redis.del('calendar:blockout:ics');
        await redis.del('calendar:blockout:json');
        console.log('✅ Blockout calendar cache cleared');
      } catch (cacheError) {
        console.error('Redis cache clear error:', cacheError);
      }
    }

    // Fetch fresh data
    const blockoutEvents = await withTimeout(
      getBlockoutCalendarData(),
      CALENDAR_FETCH_TIMEOUT_MS,
      `Blockout calendar fetch timeout after ${CALENDAR_FETCH_TIMEOUT_MS}ms`
    );
    const allCalendarEvents = processBlockoutEvents(blockoutEvents);

    // Generate and cache ICS
    const calendar = ical({ 
      name: 'Blockout Calendar',
      description: 'All blockout events',
      ttl: 300
    });
    
    allCalendarEvents.forEach(event => {
      const startDate = event.start instanceof Date ? event.start : new Date(event.start);
      const endDate = event.end instanceof Date ? event.end : new Date(event.end);
      
      calendar.createEvent({
        start: startDate,
        end: endDate,
        summary: event.title,
        description: event.description,
        location: event.location,
        url: event.url || '',
        floating: true,
        alarms: getAlarmsForEvent(event.type, event.title)
      });
    });
    
    const icsData = serializeCalendar(calendar);
    
    // Generate JSON
    const jsonData = JSON.stringify({
      calendar_name: 'Blockout Calendar',
      total_events: allCalendarEvents.length,
      events: allCalendarEvents
    }, null, 2);

    // Cache both formats
    if (redis && cacheEnabled) {
      try {
        await setCalendarCache('calendar:blockout:ics', icsData);
        await setCalendarCache('calendar:blockout:json', jsonData);
        console.log(`💾 Blockout calendar cached (${allCalendarEvents.length} events)`);
      } catch (cacheError) {
        console.error('Redis cache write error:', cacheError);
      }
    }

    res.json({
      success: true,
      message: 'Blockout calendar regenerated successfully',
      total_events: allCalendarEvents.length,
      cached: redis && cacheEnabled
    });
  } catch (error) {
    console.error('Error regenerating blockout calendar:', error);
    res.status(500).json({ 
      error: 'Error regenerating blockout calendar',
      message: error.message
    });
  }
});

// Admin calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/admin.ics', async (req, res) => {
  return res.redirect(301, '/admin/calendar?format=ics');
});

app.get('/calendar/admin', async (req, res) => {
  return res.redirect(301, '/admin/calendar?format=ics');
});

// Travel calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/travel.ics', async (req, res) => {
  return res.redirect(301, '/travel/calendar?format=ics');
});

app.get('/calendar/travel', async (req, res) => {
  return res.redirect(301, '/travel/calendar?format=ics');
});

// Blockout calendar compatibility routes (must come before /:personId routes)
app.get('/calendar/blockout.ics', async (req, res) => {
  return res.redirect(301, '/blockout/calendar?format=ics');
});

app.get('/calendar/blockout', async (req, res) => {
  return res.redirect(301, '/blockout/calendar?format=ics');
});

app.get('/calendar/google/:personId.ics', async (req, res) => {
  try {
    let { personId } = req.params;

    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    return res.redirect(301, `/calendar/${personId}?format=ics&client=google`);
  } catch (error) {
    console.error('Google ICS calendar generation error:', error);
    res.status(500).json({ error: 'Error generating Google calendar' });
  }
});

// ICS calendar endpoint (with .ics extension) - serve calendar directly
app.get('/calendar/:personId.ics', async (req, res) => {
  try {
    let { personId } = req.params;
    
    // Remove .ics extension from personId
    personId = personId.replace(/\.ics$/, '');
    
    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }

    // Redirect to main calendar endpoint with format=ics
    // This ensures we use the new Calendar Data database
    return res.redirect(301, `/calendar/${personId}?format=ics`);
  
  } catch (error) {
    console.error('ICS calendar generation error:', error);
    res.status(500).json({ error: 'Error generating calendar' });
  }
});

// Calendar endpoint using Calendar Data database only
app.get('/calendar/:personId', async (req, res) => {
  try {
    let { personId } = req.params;
    const format = req.query.format;
    const calendarClient = req.query.client === 'google' ? 'google' : 'default';
    const isGoogleClient = calendarClient === 'google';
    const regenMode = parseRegenMode(req.query.mode ?? req.query.regenMode ?? req.query.regen_mode);
    if (!regenMode) {
      return res.status(400).json({
        error: 'Invalid regen mode',
        message: 'Use mode=full | mode=events_only | mode=non_events_only'
      });
    }
    
    // Auto-detect format from Accept header for calendar subscriptions
    const acceptHeader = req.headers.accept || '';
    const shouldReturnICS = format === 'ics' || 
                           acceptHeader.includes('text/calendar') || 
                           acceptHeader.includes('application/calendar');

    // Convert personId to proper UUID format if needed
    if (personId.length === 32 && !personId.includes('-')) {
      personId = personId.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
    }
    if (!isSplitModeAllowedForPerson(personId, regenMode)) {
      return res.status(400).json({
        error: 'Regen mode not allowed for this person',
        message: `Split modes are only enabled for test person ${SPLIT_REGEN_TEST_PERSON_ID}`,
        regenMode
      });
    }

    // Check Redis cache first (if enabled, unless ?fresh=true is specified)
    const forceFresh = req.query.fresh === 'true';
    const cacheFormat = shouldReturnICS ? (isGoogleClient ? 'google_ics' : 'ics') : 'json';
    const cacheKey = buildCalendarCacheKey(personId, cacheFormat, regenMode);
    
    if (forceFresh && redis && cacheEnabled) {
      const icsKey = buildCalendarCacheKey(personId, 'ics', regenMode);
      const googleIcsKey = buildCalendarCacheKey(personId, 'google_ics', regenMode);
      const jsonKey = buildCalendarCacheKey(personId, 'json', regenMode);
      await redis.del(icsKey);
      await redis.del(googleIcsKey);
      await redis.del(jsonKey);
      const icsStillExists = await redis.exists(icsKey);
      const googleIcsStillExists = await redis.exists(googleIcsKey);
      const jsonStillExists = await redis.exists(jsonKey);
      if (icsStillExists || googleIcsStillExists || jsonStillExists) {
        console.error(`⚠️  Cache was not fully cleared for ${personId} (?fresh=true)`);
      }
    }
    
    if (redis && cacheEnabled && !forceFresh) {
      try {
        const cachedData = await redis.get(cacheKey);
        if (cachedData) {
          verboseLog(`✅ Cache HIT for ${personId} (${shouldReturnICS ? (isGoogleClient ? 'GOOGLE_ICS' : 'ICS') : 'JSON'})`);
          
          if (shouldReturnICS) {
            res.setHeader('Content-Type', 'text/calendar');
            res.setHeader('Content-Disposition', `attachment; filename="${isGoogleClient ? 'calendar-google.ics' : 'calendar.ics'}"`);
            return res.send(cachedData);
          } else {
            return res.json(JSON.parse(cachedData));
          }
        }
        logWithDedup(
          `cache_miss:person:${personId}:${regenMode}:${shouldReturnICS ? (isGoogleClient ? 'google_ics' : 'ics') : 'json'}`,
          `❌ Cache MISS for ${personId} (${shouldReturnICS ? (isGoogleClient ? 'GOOGLE_ICS' : 'ICS') : 'JSON'}, mode=${regenMode})`
        );
      } catch (cacheError) {
        console.error('Redis cache read error:', cacheError);
      }
    }
    
    // Check if Calendar Data database is configured
    if (!CALENDAR_DATA_DB) {
      return res.status(500).json({ 
        error: 'Calendar Data database not configured',
        message: 'Please set CALENDAR_DATA_DATABASE_ID environment variable'
      });
    }
    
    const result = await regenerateCalendarForPerson(personId, {
      trigger: forceFresh ? `calendar_fresh:${regenMode}` : `calendar_cache_miss:${regenMode}`,
      regenMode
    });
    if (!result.success) {
      const statusCode = result.reason === 'no_events' ? 404 : 500;
      return res.status(statusCode).json({
        error: result.reason === 'no_events' ? 'No events found' : 'Error generating calendar',
        message: result.reason === 'no_events'
          ? 'No usable event data found in Calendar Data database for this person'
          : (result.error || 'Unknown calendar generation error')
      });
    }

    if (shouldReturnICS) {
      res.setHeader('Content-Type', 'text/calendar');
      res.setHeader('Content-Disposition', `attachment; filename="${isGoogleClient ? 'calendar-google.ics' : 'calendar.ics'}"`);
      return res.send(isGoogleClient ? result.googleIcsData : result.icsData);
    }

    return res.json(result.jsonResponse);
    
  } catch (error) {
    console.error('Calendar generation error:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({ 
      error: 'Error generating calendar',
      message: error.message,
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Flight countdown page route - serves modern design
app.get('/flight/:flightId', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'public', 'flight-countdown-modern.html'));
});

// Fallback route for URL parameters (if modern design needs it)
app.get('/flight-countdown-modern.html', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'public', 'flight-countdown-modern.html'));
});

// Start background job for calendar updates
startBackgroundJob();

app.listen(port, () => {
  console.log(`Calendar feed server running on port ${port}`);
  console.log(`Background job active - updating all people after each cycle completes, then waiting ${Math.round(BACKGROUND_REFRESH_COOLDOWN_MS / 60000)} minutes`);
});
