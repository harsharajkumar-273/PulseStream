# PulseStream

A telemetry ingestion pipeline: devices POST events to an Express API, the API publishes them to Redpanda (Kafka-compatible), and a batch consumer writes them to PostgreSQL. Idempotency keys, a dead-letter topic, Prometheus metrics, and an optional Spark stage for windowed aggregates.

[![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Redpanda](https://img.shields.io/badge/Redpanda-Kafka_API-E4405F?style=flat-square)](https://redpanda.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat-square&logo=redis&logoColor=white)](https://redis.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

| | |
|---|---|
| **Measured** | 3,653 req/s average, 11 ms p50 / 36 ms p99 ingestion acknowledgments (50 connections, 30 s, local Docker, 0 errors) |
| **Also measured** | 30 s, 50-connection run through the API on a MacBook Air (Docker Desktop): 12,945 acknowledged events, all 12,945 in PostgreSQL (0 lost), 430 events/s durable, p50 180 ms / p99 854 ms from send to durable row, consumer fully drained 0.1 s after sending stopped. An earlier run on different conditions recorded 2,933 events/s with multi-second lag; it has not been reproduced — see [Benchmark](#benchmark) |
| **Also measured** | One crash-injection run: 3,000 events total, produced directly to Kafka, with 3 `SIGKILL`s during that run (consumer, Postgres, consumer). Every broker-acknowledged event had a row in PostgreSQL after the drain. The broker was never killed. See [Correctness under failure](#correctness-under-failure) |
| **Stack** | Express · KafkaJS · Redpanda · Redis · PostgreSQL · Prometheus/Grafana · Spark + Delta Lake (optional) |

---

## How it works

```mermaid
flowchart LR
    Dev[Devices] -->|POST /v1/events<br/>x-api-key + Idempotency-Key| API[Express API]
    API -->|SET NX lock| Redis[(Redis)]
    API -->|publish, key = deviceId| Raw[[metrics.raw]]
    API -.->|202 Accepted| Dev
    Raw --> Con[Batch consumer]
    Con -->|INSERT ... ON CONFLICT DO NOTHING| PG[(PostgreSQL)]
    Con -->|poison messages| DLQ[[metrics.dlq]]
    Raw --> Spark[Spark windowed aggregates<br/>to Delta Lake]
```

1. **Ingest.** The API checks the API key, validates the body with Zod, and takes a Redis `SET NX` lock on the `Idempotency-Key` so a retried request isn't published twice. It publishes to `metrics.raw` (3 partitions) keyed by `deviceId`, so each device's events stay ordered within one partition, then returns `202 Accepted`.
2. **Persist.** The consumer reads batches and writes each batch inside one PostgreSQL transaction, with a `SAVEPOINT` per message so one bad record can't take its batch neighbors down with it. `ON CONFLICT (id) DO NOTHING` makes redelivered events harmless.
3. **Isolate failures.** A message that can't be parsed or inserted rolls back to its savepoint and goes to `metrics.dlq` instead of blocking its partition or poisoning the rest of the batch. A database-level failure (e.g. the connection itself drops) rolls back the whole batch and lets KafkaJS retry with exponential backoff — Kafka offsets and DLQ sends are only acted on *after* the batch's transaction durably commits, so a crash mid-batch can't advance past events that were never actually written.
4. **Observe.** Both services expose Prometheus metrics (request counts, in-flight requests, processed and failed events, DB write duration). Grafana ships in the compose file.
5. **Aggregate (optional).** `src/spark_processor.py` reads the topic with Spark Structured Streaming and writes windowed averages, minimums, maximums, and counts to Delta Lake.

Design write-up: [`SYSTEM_DESIGN.md`](SYSTEM_DESIGN.md).

---

## Benchmark

Two scripts measure two different things. [`benchmarks/load_test.js`](benchmarks/load_test.js) (via [autocannon](https://github.com/mcollina/autocannon)) measures how fast the API answers with `202` — ingestion-edge latency. [`benchmarks/e2e_benchmark.js`](benchmarks/e2e_benchmark.js) measures how many events per second actually land a durable row in PostgreSQL, and how long that full round trip takes — the number that actually matters for a telemetry pipeline, since a fast HTTP ACK doesn't mean the event survived.

```bash
docker-compose up --build -d   # also provisions metrics.raw with 3 partitions
npm install --save-dev autocannon
node benchmarks/load_test.js       # HTTP ACK latency + req/s
node benchmarks/e2e_benchmark.js   # events actually persisted/s + true end-to-end latency
```

Latest recorded runs (50 connections, 30 s):

```
# load_test.js — HTTP ACK latency
┌─────────┬──────┬───────┬───────┬───────┬──────────┬──────────┬─────────┐
│ Stat    │ 2.5% │ 50%   │ 97.5% │ 99%   │ Avg      │ Stdev    │ Max     │
├─────────┼──────┼───────┼───────┼───────┼──────────┼──────────┼─────────┤
│ Latency │ 8 ms │ 11 ms │ 27 ms │ 36 ms │ 13.17 ms │ 23.53 ms │ 1073 ms │
└─────────┴──────┴───────┴───────┴───────┴──────────┴──────────┴─────────┘
Req/Sec avg 3,653.27 · 110k requests in 30.04 s · 2xx: 109,582 · errors: 0

# e2e_benchmark.js — events actually persisted in Postgres (MacBook Air, Docker Desktop)
Sent (202 ACKed):        12,945
Persisted in Postgres:   12,945
Lost (never persisted):  0
Send window:             30.1s
First send -> last row:  30.1s (drain after window: 0.1s)
Accepted rate:           430.7 events/s
Durable rate:            430.0 events/s
End-to-end latency p50/p95/p99 (ms): 180 / 592 / 854
```

**How to read these numbers.** The consumer kept pace with the load in this run (drain 0.1 s after the send window), so accepted rate and durable rate are the same ~430 events/s. Here the limiter is the client-to-API path (50 workers at ~115 ms per request), not the consumer or Postgres, so 430/s is a floor for this setup, not the pipeline's ceiling.

An earlier recorded run reported 88,004 events at 2,933 events/s with p50 3.7 s / p99 10.3 s latency, which implies a backlog. That run divided by the send window only and didn't time the drain, and a later re-run on a laptop didn't reproduce it (hardware and Docker settings differed, and we don't have the earlier run's environment). Don't compare the two runs or quote 2,933/s as sustained throughput. The `load_test.js` figures above are also from a separate, earlier run (11 ms p50 ACK); 50 workers finishing 430 requests/s implies roughly 115 ms per request in this run, so treat that table as unreproduced on this machine until it's re-run.

---

## Correctness under failure

The consumer used to resolve each message's Kafka offset — and send DLQ messages — *inside* the per-message loop, before the batch's PostgreSQL transaction ever reached `COMMIT`. Two related problems followed: once any single insert threw, Postgres aborted the whole transaction, so every subsequent message in that batch also failed and was misrouted to the DLQ even though it was perfectly valid; and if the process crashed between an early offset resolution and the final `COMMIT`, Kafka could advance past events whose insert was never actually durable — silent, permanent data loss.

**Fix:** a `SAVEPOINT` per message isolates one bad record from its batch neighbors, and offsets/DLQ sends are only acted on after the batch's transaction durably commits (see [`src/consumer.ts`](src/consumer.ts)).

[`benchmarks/crash_test.js`](benchmarks/crash_test.js) produces a batch of uniquely-identified events straight onto `metrics.raw` while repeatedly `SIGKILL`-ing the consumer and/or Postgres containers mid-stream (a real crash, not a graceful shutdown), then diffs every broker-acknowledged id against what's actually stored once the consumer drains:

```bash
docker-compose up --build -d
node benchmarks/crash_test.js --count 3000 --kills 3
```

```
Sent:              3000
Stored (matched):  3000
Lost:              0
Duplicated:        0
Injected crashes:  3
✅ PASS: 0 lost / 0 duplicated across 3 injected crashes.
```

What this run does and doesn't establish:

- **Scope.** It was **one run of 3,000 events in total**, not 3,000 per crash. The 3 kills happened during that one run, in a fixed order: consumer, Postgres, consumer.
- **What was checked.** Every event id whose Kafka produce call succeeded ended up with a row in `events` once consumer-group lag reached 0. In other words, a `SIGKILL` of the consumer or the database mid-batch didn't cause the consumer to commit offsets past unwritten rows. (The run above used an older version of the script that counted ids before the produce call returned. All 3,000 attempted ids were stored, so the result doesn't depend on that difference. The script now counts only acknowledged ids and prints the attempted and acknowledged totals separately.)
- **Duplicates.** `Duplicated: 0` comes from the schema, not from the experiment: `events.id` is a primary key and inserts use `ON CONFLICT DO NOTHING`. Redelivery after a crash is expected and is absorbed as a no-op.
- **Not covered.** The broker was never killed. The compose stack runs a single Redpanda node in `dev-container` mode, with no replication and relaxed fsync, so this says nothing about broker-crash durability or end-to-end exactly-once delivery. The HTTP gateway was bypassed. API-path cases, including a consumer `SIGKILL` mid-transaction, are covered by the integration suite below.

For comparison, running the same test against the pre-fix code with a single malformed message mixed into one batch — no crash required — sent 2 perfectly valid sibling events straight to the DLQ, because the poison message aborted the shared transaction and every subsequent insert in that batch inherited the same "current transaction is aborted" error.

### API-level failure tests

`benchmarks/crash_test.js` produces straight onto Kafka. [`tests/integration/api.test.mjs`](tests/integration/api.test.mjs) goes through `POST /v1/events` and tracks acknowledged ids, persisted ids, payload correctness and DLQ contents (`npm run test:integration`, against the compose stack). It covers a valid-invalid-valid batch (the invalid event is a NUL byte in `eventType`, which passes Zod but not Postgres), repeated and concurrent idempotency keys, one key with different payloads (now an explicit `422`), consumer SIGKILL mid-transaction, a failure after COMMIT but before offsets advance, a Redis outage, and a request that outlives its idempotency lock.

Against the fixed code all 11 pass. Against the pre-fix consumer the valid-invalid-valid test fails because the valid events are silently rolled back while their offsets advance; CI (`.github/workflows/integration.yml`) runs both and requires that exact result. Because the payload fingerprint lives only in Redis, a different-payload replay after Redis state loss or lock expiry is accepted by the gateway; the consumer then compares the stored row to the incoming payload, keeps the first, and dead-letters the conflict instead of silently dropping it. Writing the suite also found and fixed three more bugs: the same idempotency key silently accepted a different payload, requests hung forever during a Redis outage, and a failed DLQ send left the consumer permanently stopped while `/health` still said UP.

---

## Quick start

```bash
git clone https://github.com/harsharajkumar-273/PulseStream.git
cd PulseStream
docker-compose up --build
```

| Service | URL |
|---|---|
| Ingestion API | http://localhost:3000 |
| Consumer metrics | http://localhost:3001/metrics |
| Grafana | http://localhost:3002 |

`src/simulator.ts` sends sample device traffic, including duplicate requests to show the idempotency check returning `409 Conflict`.

---

## KEDA autoscaling

[`keda-hpa.yaml`](keda-hpa.yaml) scales the consumer on `metrics.raw` consumer-group lag. It's been deployed and proven on a real local Kubernetes cluster (not just configuration) — see [`cluster/`](cluster/) for the manifests and setup. Getting it actually working surfaced three real bugs: the broker address pointed at Redpanda's *external* listener instead of the internal one, the topic name didn't match what the app uses, and a Kubernetes DNS gotcha — **the KEDA operator resolves the broker hostname from its own pod's namespace** (`keda`), not the target deployment's, so Redpanda's advertised address had to be fully-qualified.

Measured run (300,000-event burst against a `kind` cluster):

```
t=3s    lag/threshold ratio 300/100   desired=1  running_pods=3   (spinning up)
t=15s   lag/threshold ratio 100/100   desired=3  running_pods=3   (steady-state, draining)
t=42s   lag/threshold ratio   0/100   desired=3  running_pods=3   (drained)
~5 min later (HPA's default scale-down stabilization window): back to 1 replica
```

`pulsestream-consumer-deployment` scaled from 1 → 3 real pods (capped there by `metrics.raw`'s 3 partitions — a 4th replica would have no partition to own) purely off Kafka consumer-group lag, then scaled back down once the backlog cleared. All 300,000 events landed in Postgres with 0 loss.

[`benchmarks/lag_chart.js`](benchmarks/lag_chart.js) is a faster, no-cluster stand-in against docker-compose for the same underlying mechanism: a single 300,000-event burst, scaled from 1→3 replicas mid-drain, went from ~2,972 events/s drained at 1 replica to ~8,404 events/s at 3 replicas (~2.8x).

---

## Limitations

- The benchmarks ran on one machine with everything in Docker. These are not production capacity numbers.
- `benchmarks/crash_test.js` and `benchmarks/e2e_benchmark.js` are targeted failure/latency checks, not a general automated test suite.
- The KEDA cluster proof uses a local `kind` cluster with everything (Postgres, Redis, Redpanda, consumer) redeployed in-cluster for isolation, not the exact docker-compose stack — see [`cluster/README.md`](cluster/README.md) for how it differs.

## License

MIT. See [`LICENSE`](LICENSE).
