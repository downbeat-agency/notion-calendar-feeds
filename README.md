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
CALENDAR_FEED_HISTORY_CUTOVER_DATE=2026-08-02 # fixed Postgres authority boundary
```

Shadow diagnostics include only occurrence counts, type counts, pairing-method counts, and content hashes. Main events pair first by a private canonical source identity supplied by Downbeat; that identity is removed from public JSON artifacts. Diagnostics do not log names, titles, notes, locations, source identities, or booking details.

Personal shadow comparisons freeze the already-published events before the fixed history cutover date in a no-expiry Redis compatibility snapshot. Postgres mode combines that frozen history with Postgres events on and after the boundary, fails closed when a subscriber's history was not frozen, uses a separate Redis namespace, assigns stable ICS UIDs supplied by Downbeat, preserves last-known-good calendar artifacts on downstream failures, and disables the Notion fleet sweep. It never imports historical Payroll Personnel records. The existing calendar URLs—including personal, Google, Admin, Travel, and Blockout routes—do not change.

Cached Postgres artifacts are validated against Downbeat's durable calendar source revision on every subscription request. A relevant edit therefore rebuilds on the next client poll rather than waiting for `CACHE_TTL`. Shadow comparisons are retained in Redis for seven days and are available through the service-key-authenticated `/api/internal/calendar-shadow-report`; `/api/internal/calendar-shadow-run` starts a complete shadow audit while Notion remains the served source.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.
