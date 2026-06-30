# PulseStream: System Design & Technical Specification

This document provides a detailed breakdown of the architectural decisions, data structures, and patterns implemented in **PulseStream** to handle high-throughput, real-time activity and IoT metrics ingestion.

---

## 1. Technical Stack Primitives

*   **API Gateway Node Runtime:** Node.js (v20+ / TypeScript) using Express for routing and Zod for edge-level validation contracts.
*   **State Store & Caching:** Redis (v7+) used as a sub-millisecond key-value registry for API Key metadata caching, rate-limit buckets, and atomic idempotency locks.
*   **Message Broker (Event Bus):** Redpanda (Kafka v3 API compatible) configured with log-structured partition brokers.
*   **Worker Ingestion Engine:** Node.js consumer utilizing the `kafkajs` engine for batch consumption, thread heartbeats, and transactional SQL flushing.
*   **Cold Database Store:** PostgreSQL (v15+) optimized for structured, indexed metrics storage with database-level primary keys enforcing deduplication.
*   **Observability Stack:** Prometheus for metrics scraping, and Grafana for monitoring HTTP request durations, error rates, and consumer partition lag.

---

## 2. Ingestion Flow Pipeline (Chronological)

The following diagram illustrates the lifecycle of an event from client request to database persistence:

```
[IoT Device Client]
        │
        ▼
[1. HTTP POST Gateway] ──(Authentication Middleware)──> Check Redis Cache
        │                                                     │ (Cache Miss)
        │                                                     ▼
        │                                              Query PostgreSQL
        │
        ▼
[2. Idempotency Guard] ──(Redis SETNX Lock)──────────> Lock key for 10s
        │
        ▼
[3. Payload Validation] ──(Zod schema compile)────────> Validate schema & clock drift
        │
        ▼
[4. Kafka Producer] ────> Route to Redpanda topic `metrics.raw` (Partition key: `deviceId`)
        │
        ▼
[5. Ingest Queue] ──────> Partition buffers sequentially on disk
        │
        ▼
[6. Batch Consumer] ────> Reads batch of 100 messages -> Opens PG Client Connection
        │
        ▼
[7. DB Transaction] ────> BEGIN -> INSERT with `ON CONFLICT DO NOTHING` -> COMMIT
```

---

## 3. Core Architectural Sub-Systems

### A. Read-Through Authentication Cache
To protect our PostgreSQL connection pool from getting saturated by authentication queries under high load, we isolate client credential verification at the edge:

1.  The client transmits their unique API Key in the `x-api-key` header.
2.  The gateway performs a `GET client:key:<key>` check against Redis.
    *   **Cache Hit:** Retrieves client metadata (e.g., ID, name) and proceeds.
    *   **Cache Miss:** Performs a query on the PostgreSQL `clients` table.
        *   If the key is valid and active, it is saved back to Redis using `SETEX client:key:<key> 300 <json_metadata>` (5-minute Time-To-Live).
        *   If the key is invalid or inactive, the request is immediately rejected with an **HTTP 411 Unauthorized**.

### B. Distributed Edge Idempotency Caching
To achieve exactly-once processing side-effects from at-least-once network configurations, the gateway requires clients to supply a unique `Idempotency-Key` UUIDv4 header:

1.  **Atomic Lock Attempt:** The gateway executes a Redis `SET idempotency:key:<UUID> IN_PROGRESS EX 10 NX` command.
    *   `NX` ensures the command succeeds *only* if the key does not already exist (mutual exclusion).
    *   `EX 10` sets an automatic 10-second expiration lock to release the key if the gateway node crashes mid-request.
2.  **Handling Conflicts:**
    *   If the command fails (returns `null`), we query the key's state:
        *   State is `IN_PROGRESS`: Returns **HTTP 409 Conflict** (indicating another thread is currently processing this identical request).
        *   State is `RESOLVED:<response>`: Returns the cached status and payload directly without repeating any downstream operations.
3.  **Response Interception:**
    *   If the lock is acquired, the gateway wraps Express's `res.json` method.
    *   On response, if the HTTP code is `< 500` (success or client error), we update the Redis key to `RESOLVED:<status>:<payload>` with a 24-hour TTL (`EX 86400`).
    *   If it is a `5xx` server error, we delete the key (`DEL`) to allow the client to retry immediately.

### C. Partition-Keyed Event Broker
*   **Decoupling:** Writing events directly to PostgreSQL from the web gateway is an architectural bottleneck. By placing **Redpanda** in front, we isolate the web thread from slow database write locks.
*   **Partitioning Guarantee:** Message brokers distribute events across partitions. In PulseStream, the message `key` is set to the client's `deviceId`. Kafka hashes the key to assign the partition:
    $$\text{Partition} = \text{Hash}(key) \pmod{\text{Total Partitions}}$$
    This ensures that all events for a given device are sequentially routed to the same partition, guaranteeing they are processed in strict chronological order by the consumer.

### D. High-Throughput Batch Transaction Worker
The consumer daemon reads messages from Redpanda. To scale throughput, it uses batch ingestion:

1.  The consumer retrieves a batch of events (e.g. 50–200 messages).
2.  Instead of opening a new transaction per message, it borrows a connection from the `pg.Pool`, starts a single database transaction (`BEGIN`), and inserts all messages sequentially.
3.  **Conflict Deduplication:** If an event has already been stored (due to redeliveries), the insert runs `ON CONFLICT (id) DO NOTHING`.
4.  **Failure Isolation:** If a database error occurs, the entire batch is rolled back (`ROLLBACK`), maintaining database consistency, and the consumer does not commit offsets.

---

## 4. Disaster Recovery & Fallback Modes

| Failure Scenario | Impact | System Fallback Mode |
|---|---|---|
| **Redis Crash** | Gate-keeping (auth cache, idempotency) goes offline. | **Circuit Breaker Activates:** The gateway bypasses Redis. It validates API keys by querying PostgreSQL directly (utilizing a local, short-lived in-memory cache to prevent database DDOS). Idempotency checks are skipped at the gateway and safely handled by PostgreSQL unique constraints. |
| **PostgreSQL Outage** | Consumer worker cannot write events. | **Queue Buffering:** The Consumer transaction fails and rolls back. The worker stops committing offsets to Redpanda. Redpanda continues buffering the stream on disk. Once PostgreSQL recovers, the worker picks up from the last committed offset and catches up. **No data is lost.** |
| **Redpanda Broker Partition Split** | Gateway cannot publish events. | **Local Backpressure:** Gateway connection buffers fill up. The gateway responds to clients with **HTTP 503 Service Unavailable**, forcing clients to buffer locally and retry with exponential backoff. |
