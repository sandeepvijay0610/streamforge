# StreamForge

**Real-time transaction data pipeline.** Ingests webhooks, buffers through Kafka, validates and normalizes with Python consumers, persists to PostgreSQL, and surfaces live analytics and anomaly detection through SQL and Streamlit.

Everything runs locally with `docker compose up`.

---

## Architecture

```
     External Provider (Postman / curl / simulator)
                        │
                        │  POST /v1/webhooks/{provider}
                        ▼
              ┌──────────────────────┐
              │  FastAPI Ingestion   │  thin: validate, envelope, publish
              └──────────┬───────────┘
                         │  key = event_id
                         ▼
              ┌──────────────────────┐
              │ transactions.raw.v1  │  immutable evidence
              └──────────┬───────────┘
                         │
              ┌──────────┴───────────┐
              │                      │
              ▼                      ▼
     ┌────────────────┐    ┌────────────────────┐
     │  ETL Consumer  │    │  Raw Archiver      │
     │  ───────────   │    │  (MinIO/S3, V2)    │
     │  adapter       │    └────────────────────┘
     │  normalize     │
     │  dedup state   │
     └────────┬───────┘
              │
       ┌──────┴───────┬───────────────┐
       │              │               │
       ▼              ▼               ▼
  cleaned.v1     failed.v1       retry.v1
  key=customer   permanent       transient
       │         data errors     infra errors
       │
       ├─────────────────┐
       │                 │
       ▼                 ▼
 ┌──────────────┐  ┌───────────────┐
 │ DB Consumer  │  │ Fraud Detector│
 └──────┬───────┘  └───────┬───────┘
        │                  │
        ▼                  ▼
   ┌────────────┐    ┌──────────────┐
   │ PostgreSQL │    │anomaly_alerts│
   └─────┬──────┘    └──────────────┘
         │
         ▼
   ┌────────────┐
   │ Streamlit  │
   └────────────┘
```

---

## Core design principles

> **Raw events may vary. Cleaned events may not.**

> **Bad data goes to `failed`; temporary infrastructure failures go to `retry`.**

> **Provider-specific complexity terminates at the adapter boundary.**

---

## Quick start

Requires Docker Desktop. Nothing else.

```bash
git clone <repo>
cd streamforge
docker compose up -d --build
```

| Service | URL | Purpose |
|---|---|---|
| Streamlit dashboard | http://localhost:8501 | Live analytics + anomaly alerts |
| FastAPI docs | http://localhost:8000/docs | Interactive API |
| Redpanda Console | http://localhost:8080 | Inspect Kafka topics/messages/lag |
| PostgreSQL | localhost:5432 | `streamforge` / `streamforge` |

Send a test event:

```bash
curl -X POST http://localhost:8000/v1/webhooks/provider-b \
  -H "Content-Type: application/json" \
  -d @example_events/smoke.json
```

Within ~10 seconds the dashboard shows the transaction in the live feed.

---

## Demo

![StreamForge dashboard](docs/dashboard.png)

*(Screenshot: KPI header, per-minute volume, merchant ranking, live feed, anomaly alerts.)*

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion API | FastAPI + Pydantic |
| Message broker | Apache Kafka (KRaft mode) |
| Consumers | Python, `confluent-kafka` |
| Storage | PostgreSQL 16 |
| Analytics | SQL (window functions, CTEs, FILTER) |
| Dashboard | Streamlit |
| Infra | Docker, Docker Compose |

---

## Key engineering decisions

**Versioned topics, not versioned code paths.** `transactions.raw.v1` / `.cleaned.v1` / `.failed.v1` / `.retry.v1`. A future schema break introduces `.v2` alongside `.v1`; existing consumers keep running unchanged.

**Money as `amount_minor` BIGINT.** Never float. `₹1299.50` → `129950`. V1 limits to 2-decimal currencies; the storage model is V2-safe for JPY/KWD via a future currency metadata table.

**`event_id` is the dedup key. `transaction_id` is not.** One business transaction can legitimately produce multiple events (`payment.created`, `payment.succeeded`, `payment.refunded`) with the same `transaction_id` but distinct `event_id`s. `event_id` is unique; `transaction_id` is indexed but not unique.

**Persistent dedup state machine.** `processed_events` table with states `PROCESSING / SUCCEEDED / FAILED_RETRYABLE / FAILED_PERMANENT`. Atomic claim via `INSERT ... ON CONFLICT DO NOTHING RETURNING`. 60-second lease timeout so crashed workers can be reclaimed. The registry is behind a `DedupStore` interface — V1 is Postgres, V2 can be Redis without touching the consumer.

**Duplicate semantics are explicit.**

- Exact duplicate of a successful event → silent drop.
- Exact duplicate of a failed event → drop with a warning.
- Same `event_id`, different payload → route to `failed.v1` as `EVENT_ID_CONFLICT`. Never overwrite.

**Provider adapters, not provider conditionals.** Every source maps to one canonical schema through a `TransactionAdapter`. Adding Provider C requires a new adapter, new tests, and zero changes to Kafka, Postgres, analytics, or the dashboard.

**Two-topic key boundary.** `raw.v1` is keyed by `event_id` (customer identity isn't known before the adapter runs). `cleaned.v1` is keyed by `customer_id` (per-customer ordering, which the fraud rules depend on). The key changes at the normalization boundary by design.

**Failed vs retry split.** Data errors go to `failed.v1`. Infrastructure errors (DB down, Kafka unavailable) go to `retry.v1`. The DB consumer retries transient errors inline with exponential backoff; exhausted retries route to a dedicated retry worker.

---

## Load test

| Rate | Duration | Events | Accepted | ETL lag | DB lag |
|--:|--:|--:|--:|--:|--:|
| 50/s | 60s | 3,000 | 100% | 0 | 0 |
| 200/s | 30s | 6,000 | 100% | 0 | 0 |

Details and methodology: [`docs/load-test.md`](docs/load-test.md).

---

## Documentation

- **[V1 data contract](docs/v1-contract.md)** — schemas, topics, error taxonomy, dedup rules, upgrade strategy
- **[Architecture](docs/architecture.md)** — component responsibilities, failure semantics, what's deliberately not in V1
- **[Load test](docs/load-test.md)** — methodology and results
- **[Example events](example_events/)** — fixtures used in tests and manual smoke tests

---

## What's not in V1

Documented deliberately, so nobody mistakes absence for oversight:

- **Raw archival to object storage.** MinIO/S3 consumer is designed (`archiver/`), not built. V1 keeps raw in Kafka for replay.
- **Automated replay tooling.** Failed and retry events remain in Kafka topics; replay is a manual operation in V1.
- **Exactly-once processing.** V1 targets at-least-once with idempotent downstream writes. The transaction table's `UNIQUE(event_id)` handles the last mile.
- **Schema registry.** No Avro/Protobuf. JSON with Pydantic validation. Appropriate for V1 scale.
- **Multi-broker Kafka.** Single broker, 3 partitions per topic. Horizontal scaling is a config change, not a rewrite.

---

## Repository layout

```
streamforge/
├── ingestion/          FastAPI service
├── etl/                Adapter + normalize + dedup consumer
├── db-consumer/        cleaned.v1 → PostgreSQL
├── retry-consumer/     retry.v1 → PostgreSQL (bounded retries)
├── fraud-detector/     SQL anomaly rules → anomaly_alerts
├── dashboard/          Streamlit
├── database/           init.sql, views, seed, fraud-rule support
├── scripts/            load generator
├── example_events/     fixtures
└── docs/               architecture, contract, load test
```
