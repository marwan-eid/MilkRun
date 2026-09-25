# 🥛 The Milk-Run — Live Delivery Tracker

[![MilkRun Full-Stack CI](https://github.com/marwan-eid/MilkRun/actions/workflows/ci.yml/badge.svg)](https://github.com/marwan-eid/MilkRun/actions/workflows/ci.yml)
**[Live Demo: marwan-milkrun.duckdns.org](https://marwan-milkrun.duckdns.org/)**

A real-time, event-driven fleet tracker. A simulator drives 50 electric delivery vans through Amsterdam (Picnic-style milkman routes) and streams their GPS pings, deliveries and route plans through Kafka into a reactive Spring WebFlux pipeline, which predicts every van's arrival at its next stop, flags deliveries at risk of missing their slot, and streams the fleet to a React map.

## Architecture

```
┌──────────────────┐            ┌─────────────────────────────────────────────────────────────┐
│ Simulator (TS)   │ gps-events │ Backend (Java 21, Spring WebFlux, Project Reactor)          │
│ 50 vans, OSRM    │───────────▶│ dedup (sliding window) → reorder buffer (3 s grace)         │
│ street routes    │ delivery-  │   → ETA along the planned route (zones, learned speed)      │
│ chaos: jitter,   │ events     │   → SSE (sampled 2/s per van) ──────────────────▶ React map │
│ retries, dead    │───────────▶│ delivery log, SLA breaches, GPS track (R2DBC, idempotent)   │
│ zones + backfill │ route-plans│ late events → reconciled into the track + dead-letter log   │
│                  │───────────▶│ route plans (compacted topic, rebuilt at startup)           │
│                  │◀───────────│ dispatch: cheapest insertion with SLA checks                │
│                  │ dispatch-  │ Apache Calcite: PostgreSQL + live fleet in one SQL query    │
└──────────────────┘ events     └──────────────┬──────────────────────────────┬───────────────┘
                                               │                              │
                                  ┌────────────▼────────────┐    ┌────────────▼────────────┐
                                  │ PostgreSQL 16 + PostGIS │    │ Prometheus + Grafana    │
                                  │ (Flyway migrations)     │    │ (alert rules, dashboard)│
                                  └─────────────────────────┘    └─────────────────────────┘
```

A detailed walkthrough of every component is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 📸 Dashboard Previews

### 1. Live Fleet Monitor (Map & SLA Engine)
Vans move along real street geometry; colour shows the SLA risk of each van's next stop. Right-click the map to dispatch an ad-hoc order and see which van the backend picked.

![Live Map Dashboard](./docs/screenshots/live-map-dashboard.png)

### 2. Fleet Analytics
SQL through Apache Calcite, including federated queries that join the live in-memory fleet with delivery history in PostgreSQL.

![Analytics Preview](./docs/screenshots/fleet-analytics.png)

## What it does

### ETA and SLA risk
Each van's route plan (street polyline, stops, delivery slots) is published to a compacted Kafka topic. The backend map-matches every GPS ping onto the van's current leg, walks the remaining road to the next stop at the van's learned free-flow speed, slowing each segment by the PostGIS delay zone it lies in, and compares the predicted arrival with the stop's deadline: **NONE**, **WARNING** (under a minute to spare) or **CRITICAL** (projected late). A Resilience4j circuit breaker guards the estimator; when it is open, or the van is off its planned route, a straight-line × 1.3 estimate is used. ETA accuracy is measured on every arrival (`milkrun.eta.error.seconds`).

### Handling messy telemetry
The simulator reproduces what cellular devices do: per-ping network jitter (natural reordering), retried sends (~5% duplicates), and dead zones where the van buffers pings and uploads the backlog after reconnecting.
- **Deduplication:** exact sliding-window anti-replay per van (the IPsec technique, RFC 6479): no false positives, 512 bytes per van. A Bloom filter implementation is kept as an alternative; see the [benchmark](docs/BENCHMARKS.md).
- **Reordering:** per-van priority queue with a 3 s grace window, released by a single timer thread so every van's events stay in order.
- **Late events:** events older than what was already shown are reconciled into the van's recorded GPS track and logged in the dead-letter log; malformed records go to the `gps-events-dlq` topic.

### Processing each event once
Kafka offsets are committed only after an event has been handled (deferred commits, since the reorder buffer finishes events out of order). Database writes are idempotent (keyed by event id), delivery records are processed in order per partition, and a record that keeps failing is dead-lettered rather than blocking its partition.

### Dispatch
Right-click the map to create an order. The backend tries it before every remaining stop of every van, costs each insertion as extra distance plus penalties for stops it would make late, publishes the cheapest assignment to that van, and the simulator reroutes the van without backtracking.

### Analytics with Apache Calcite
Two schemas behind one SQL connection: `milkrun` (PostgreSQL through Calcite's JDBC adapter; joins and aggregates are pushed down) and `live` (the fleet's in-memory state). Queries run on a dedicated thread, off the reactive event loop.

### Scaling out
With `MILKRUN_FANOUT_MODE=kafka`, several backend instances split the Kafka partitions and share van state through a `van-state` topic, so each serves the whole fleet. `scripts/scale-demo.sh` runs two instances and verifies the split and the failover.

### Operations
- GitHub Actions: tests on every push; after CI passes on `master`, images are built, pushed to GHCR and rolled out to the VM over SSH with a health gate and automatic rollback (`.github/workflows/deploy.yml`, needs `DEPLOY_*` secrets).
- A scheduled workflow probes the live demo every 15 minutes and fails (GitHub emails) when the pipeline is degraded.
- `/api/observability/health` reports rates over the last 5 minutes, end-to-end latency percentiles and a status; Prometheus alert rules and a provisioned Grafana dashboard ship in `infra/`.

## Tech Stack

| Layer | Technologies |
|---|---|
| **Messaging** | Apache Kafka (KRaft locally, Zookeeper in production) |
| **Backend** | Java 21, Spring Boot 3.3, WebFlux, Project Reactor, reactor-kafka |
| **Database** | PostgreSQL 16 + PostGIS, R2DBC, Flyway |
| **Analytics** | Apache Calcite 1.37 |
| **Resilience** | Resilience4j circuit breaker, token-bucket rate limiting |
| **Metrics** | Micrometer, Prometheus, Grafana |
| **Frontend** | React 19, TypeScript, Leaflet, Server-Sent Events, Vite |
| **Simulator** | TypeScript, KafkaJS, OSRM street routing |
| **Testing** | JUnit 5, Testcontainers (Kafka, PostGIS), JMH, Vitest |
| **Delivery** | Docker, GitHub Actions, GHCR, Caddy (TLS) |

## Quick Start

Prerequisites: Docker, JDK 21 (or 17 with `-P local-dev`), Maven 3.9+, Node.js 22.

```bash
# 1. Infrastructure: Kafka, PostgreSQL/PostGIS, Prometheus, Grafana
docker compose up -d

# 2. Backend (applies the Flyway migrations on start)
cd backend && mvn spring-boot:run

# 3. Simulator
cd simulator && npm install && npm start     # VAN_COUNT=10 npm start for fewer vans

# 4. Dashboard
cd frontend && npm install && npm run dev    # http://localhost:5173
```

Grafana: http://localhost:3001 (admin / milkrun), dashboard "MilkRun operations". Prometheus: http://localhost:9090.

Production-style stack (all services in containers, Caddy on 80/443): `docker compose -f docker-compose.prod.yml up -d --build`; add `--profile monitoring` for Prometheus and Grafana on localhost.

### Useful settings

| Variable | Default | Effect |
|---|---|---|
| `VAN_COUNT` (simulator) | 50 | Number of vans |
| `TIME_SCALE` (simulator) | 4 | Simulated seconds per real second (speeds are reported in simulated km/h) |
| `ROUTER=straight` (simulator) | OSRM | Straight-line streets, no calls to the public OSRM server |
| `CHAOS_ENABLED` (simulator) | true | Jitter, retries and dead zones |
| `MILKRUN_FANOUT_MODE` (backend) | local | `kafka` to run several backend instances |
| `milkrun.pipeline.dedup-strategy` | window | `bloom` for the Bloom filter |
| `milkrun.pipeline.reorder-buffer-grace-ms` | 3000 | Reorder window (and most of the end-to-end latency) |

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/stream/vans` | SSE stream of van state (≤ 2 updates/s per van) |
| `GET /api/vans`, `GET /api/vans/{id}` | Current fleet snapshot / one van |
| `POST /api/dispatch` | Ad-hoc order `{latitude, longitude}` → assignment (202), 409 if no van, 429 if rate limited |
| `GET /api/analytics/at-risk-vans` | Federated: vans at risk now + their breach history |
| `GET /api/analytics/live-zone-risk` | Federated: vans in delay zones now + each zone's history |
| `GET /api/analytics/delay-zones`, `sla-summary`, `van-performance`, `dlq-summary`, `heatmap` | Historical analytics |
| `GET /api/observability/health` | Status over the last 5 min, latency percentiles, counters |
| `GET /api/observability/ready`, `/live` | Readiness (503 until ready) and liveness |
| `GET /actuator/prometheus` | Prometheus metrics |

## Tests

```bash
cd simulator && npm test      # 41 tests, no network
cd frontend && npm test       # 13 tests
cd backend && mvn test        # 100 tests; Testcontainers (Kafka, PostGIS) needs Docker
```

Benchmarks: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Project Structure

```
MilkRun/
├── simulator/            TypeScript fleet simulator: routes, vans, chaos (device link), dispatch consumer
├── backend/              Spring WebFlux engine
│   └── src/main/java/com/milkrun/
│       ├── api/          SSE, REST, dispatch, analytics, observability controllers
│       ├── calcite/      Calcite federation (PostgreSQL + live fleet)
│       ├── consumer/     GPS and delivery pipelines, late events, Kafka dead letters
│       ├── dispatch/     Cheapest-insertion dispatcher
│       ├── engine/       ETA engine, route plans and geometry, geofencing
│       ├── fleet/        Fleet view (single instance or shared through Kafka)
│       ├── observability/ Rolling pipeline health
│       ├── persistence/  GPS archive, dead-letter log
│       └── pipeline/     Deduplicators, reorder buffer
├── frontend/             React dashboard (see frontend/README.md)
├── infra/                PostgreSQL init, Prometheus config and alert rules, Grafana provisioning and dashboard
├── scripts/              deploy-remote.sh (used by CD), scale-demo.sh
└── docs/                 Architecture, benchmarks, screenshots
```
