# MilkRun dashboard

React 19 + TypeScript + Leaflet front end for the MilkRun fleet tracker.

- **Live map**: every van as a marker coloured by SLA risk (red: projected to
  miss its next delivery slot, amber: less than a minute to spare), rotated to
  its heading, gliding between the twice-a-second updates.
- **Fleet panel**: vans grouped by risk, with ETA to the next stop and slack
  against its slot.
- **Dispatch**: right-click (long-press on touch screens) anywhere to drop an
  ad-hoc order. The backend picks the van and the position in its route; the
  map shows its choice, the detour and the predicted arrival.
- **Analytics**: Apache Calcite queries, including federated ones that join
  the live fleet with delivery history in PostgreSQL.
- **Header**: connection state, van count and end-to-end latency (device
  reading to map, p50).

## Data flow

`useVanStream` opens an `EventSource` on `/api/stream/vans`, loads a snapshot
from `/api/vans` on every (re)connect, batches updates at ~15 fps, reconnects
with exponential backoff and drops vans that have been silent for two minutes.

## Development

```bash
npm install
npm run dev        # http://localhost:5173, proxies /api to localhost:8080
npm test           # Vitest (pure helpers in src/lib)
npm run lint       # oxlint
npm run build      # typecheck + production build
```

Set `VITE_API_URL` to point the dashboard at a backend on another origin (the
backend must allow that origin in `milkrun.cors.allowed-origins`). In the
Docker image, nginx serves the build and proxies `/api` to the backend, so no
CORS is involved.
