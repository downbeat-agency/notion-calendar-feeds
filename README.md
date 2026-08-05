# Downbeat Calendar Feeds

A Node.js compatibility edge for Downbeat calendar subscriptions. Existing public subscription URLs and ICS presentation stay here; the backing data source can be Notion or the Downbeat Postgres projector.

## Getting Started

### Prerequisites
- Node.js 20 or higher
- npm or yarn

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```

3. Run the application:
   ```bash
   npm start
   ```

## Usage

The source defaults to Notion. A safe rollout uses these values in order:

```text
CALENDAR_FEED_SOURCE=notion   # current behavior
CALENDAR_FEED_SOURCE=shadow   # serve Notion, compare Postgres in the background
CALENDAR_FEED_SOURCE=postgres # serve Postgres through the same URLs and renderer
```

`shadow` and `postgres` also require:

```text
CALENDAR_FEED_API_BASE_URL=https://<downbeat-app-host>
CALENDAR_FEED_SERVICE_KEY=<same high-entropy secret configured in Downbeat>
CALENDAR_FEED_API_TIMEOUT_MS=25000 # optional
```

Postgres mode publishes January 1 of the current Los Angeles business year through all future records. There is no rolling future cutoff and no frozen Redis history. Every request reads a cached artifact only when its Postgres source revision and deployment-aware renderer version still match; otherwise it rebuilds from Postgres. Concurrent requests for the same projection share one in-flight build.

Calendar times default to floating wall-clock values, so a 3:00 PM event remains 3:00 PM in every subscriber timezone. The compatibility rollback is explicit:

```text
CALENDAR_TIME_MODE=floating  # default
CALENDAR_TIME_MODE=legacy-la # temporary rollback only
```

Blockouts are emitted as true all-day events. Responses include `ETag` and `Last-Modified`, allowing calendar clients to receive `304 Not Modified` when nothing changed. Optional operational settings are:

```text
CALENDAR_RENDERER_VERSION=<deployment-id>        # otherwise Railway/Render/Git SHA is used
CALENDAR_HEALTH_PROBE_INTERVAL_MS=300000
CALENDAR_SLOW_BUILD_MS=5000
CALENDAR_OPS_ALERT_WEBHOOK_URL=https://<private-webhook>
CALENDAR_ALERT_COOLDOWN_MS=300000
```

Shadow diagnostics include only occurrence counts, type counts, pairing-method counts, and content hashes. Main events pair first by a private canonical source identity supplied by Downbeat; that identity is removed from public JSON artifacts. Diagnostics do not log names, titles, notes, locations, source identities, or booking details.

Postgres mode uses a separate Redis namespace, assigns stable ICS UIDs supplied by Downbeat, preserves last-known-good calendar artifacts on downstream failures, and disables the Notion fleet sweep. It never imports historical Payroll Personnel records. The existing calendar URLs—including personal, Google, Admin, Travel, and Blockout routes—do not change.

Cached Postgres artifacts are validated against Downbeat's durable calendar source revision on every subscription request. A relevant edit therefore rebuilds on the next client poll rather than waiting for `CACHE_TTL`. Shadow comparisons are retained in Redis for seven days and are available through the service-key-authenticated `/api/internal/calendar-shadow-report`. The report exposes the complete occurrence and field-drift metrics separately under each `byKind` bucket, so personal-calendar findings are not mixed with Admin, Travel, or Blockout findings. `/api/internal/calendar-shadow-run` reads the subscriber list from Notion's Calendar Data database, verifies both Event and Rehearsal formula completeness against stable Payroll Personnel membership witnesses, refreshes every Notion calendar into a separate no-expiry audit snapshot, retries failed personnel refreshes once at concurrency one, and compares each completed snapshot with Postgres while Notion remains the served source. A later successful refresh replaces the snapshot; the public delivery cache keeps its normal short TTL.

Maintenance, regeneration, cache-clearing, diagnostics, and `/api/internal/calendar-health` require `X-Downbeat-Calendar-Service-Key`. Mutating maintenance routes use `POST` or `DELETE`; public subscription pages never invoke them.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.
