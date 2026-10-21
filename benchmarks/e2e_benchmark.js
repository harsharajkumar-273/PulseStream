#!/usr/bin/env node
/**
 * End-to-end persistence benchmark.
 *
 * `load_test.js` measures how fast the gateway answers with HTTP 202 —
 * that's ingestion-edge latency, not delivery. This script measures the
 * thing that actually matters for a telemetry pipeline: how many events
 * per second make it all the way through (gateway -> Redpanda -> consumer
 * batch insert -> durable Postgres row), and how long that round trip
 * takes end to end.
 *
 * Method: POST real events to /v1/events (same path production traffic
 * takes) with a client-side send timestamp embedded in each payload, wait
 * for the consumer to drain, then look up each event's row in Postgres and
 * diff `created_at` against the send timestamp.
 *
 * Two rates are reported, because they answer different questions:
 *   - accepted rate: events persisted / send window. This is how fast the
 *     gateway accepted work that eventually became durable, NOT a sustained
 *     durable-write rate — under a burst the consumer is still draining a
 *     backlog after the send window closes.
 *   - durable rate: events persisted / (first send -> last durable row).
 *     This includes the drain and is the closer number to steady-state
 *     write throughput for this burst.
 *
 * Requires the stack to already be running: `docker-compose up --build -d`.
 *
 * Usage:
 *   node benchmarks/e2e_benchmark.js [--duration 30] [--connections 50] [--url http://localhost:3000/v1/events]
 */
import crypto from "crypto";
import { spawnSync } from "child_process";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const url = arg("url", "http://localhost:3000/v1/events");
const duration = Number(arg("duration", 30));
const connections = Number(arg("connections", 50));
const apiKey = arg("api-key", "ps_live_test_key_abc123xyz");
const POSTGRES_CONTAINER = "pulsestream-postgres";

function psql(sql) {
  const result = spawnSync(
    "docker",
    ["exec", "-i", POSTGRES_CONTAINER, "psql", "-U", "postgres", "-d", "pulsestream", "-t", "-A"],
    { input: sql, encoding: "utf8" }
  );
  if (result.status !== 0) {
    throw new Error(`psql failed: ${result.stderr || result.error}`);
  }
  return result.stdout.trim();
}

function consumerLag() {
  const out = spawnSync(
    "docker",
    ["exec", "pulsestream-redpanda", "rpk", "group", "describe", "pulsestream-metrics-group"],
    { encoding: "utf8" }
  ).stdout;
  const line = (out || "").split("\n").find((l) => l.trim().startsWith("metrics.raw"));
  if (!line) return null;
  const lag = Number(line.trim().split(/\s+/)[4]);
  return Number.isFinite(lag) ? lag : null;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function percentile(sorted, p) {
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

async function main() {
  console.log(`🚀 Driving real traffic through ${url} for ${duration}s @ ${connections} concurrent requests...\n`);

  const sent = new Map(); // id -> clientSendTimeMs
  let acked = 0;
  let rejected = 0;
  const stop = Date.now() + duration * 1000;

  async function worker() {
    while (Date.now() < stop) {
      const id = crypto.randomUUID();
      const sendTimeMs = Date.now();
      const body = JSON.stringify({
        deviceId: crypto.randomUUID(),
        eventType: "temperature",
        value: Math.random() * 100,
        timestamp: sendTimeMs,
      });
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-api-key": apiKey,
            "Idempotency-Key": id,
          },
          body,
        });
        if (res.status === 202) {
          sent.set(id, sendTimeMs);
          acked++;
        } else {
          rejected++;
        }
        await res.text();
      } catch {
        rejected++;
      }
    }
  }

  const workerStart = Date.now();
  await Promise.all(Array.from({ length: connections }, () => worker()));
  const sendWindowMs = Date.now() - workerStart;

  console.log(`📨 Sent ${acked} events (${rejected} rejected) in ${(sendWindowMs / 1000).toFixed(1)}s.`);
  console.log("⏳ Waiting for the consumer to drain (Kafka group lag -> 0)...");

  const drainStart = Date.now();
  let lagZeroStreak = 0;
  let drained = false;
  while (Date.now() - drainStart < 120_000) {
    const lag = consumerLag();
    if (lag === 0) {
      lagZeroStreak++;
      if (lagZeroStreak >= 3) {
        drained = true;
        break;
      }
    } else {
      lagZeroStreak = 0;
    }
    await sleep(1500);
  }

  // Chunked: a single query embedding tens of thousands of UUIDs can overflow
  // the child process's stdin pipe buffer (ENOBUFS) when written all at once.
  const ids = [...sent.keys()];
  const CHUNK = 2000;
  const rows = [];
  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    const out = psql(
      `SELECT id, EXTRACT(EPOCH FROM created_at) * 1000 FROM events WHERE id = ANY(ARRAY[${chunk
        .map((id) => `'${id}'`)
        .join(",")}]::uuid[]);`
    )
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        const [id, createdAtMs] = line.split("|");
        return { id, createdAtMs: Number(createdAtMs) };
      });
    rows.push(...out);
  }

  const foundIds = new Set(rows.map((r) => r.id));
  const lost = ids.filter((id) => !foundIds.has(id));

  const latencies = rows
    .map((r) => r.createdAtMs - sent.get(r.id))
    .filter((l) => Number.isFinite(l))
    .sort((a, b) => a - b);

  const persistedCount = rows.length;
  const wallClockSeconds = sendWindowMs / 1000;
  const acceptedPerSec = persistedCount / wallClockSeconds;
  // Measured from the rows themselves (not from the lag poll, which adds up
  // to ~4.5s of polling slack), so it's when the last event actually landed.
  const lastDurableMs = rows.reduce((m, r) => Math.max(m, r.createdAtMs), workerStart);
  const durableSeconds = (lastDurableMs - workerStart) / 1000;
  const durablePerSec = persistedCount / durableSeconds;

  console.log("\n=== End-to-end persistence benchmark result ===");
  console.log(`Target: ${url}`);
  console.log(`Duration: ${duration}s, Connections: ${connections}`);
  console.log(`Sent (202 ACKed):        ${acked}`);
  console.log(`Persisted in Postgres:   ${persistedCount}`);
  console.log(`Lost (never persisted):  ${lost.length}`);
  console.log(`Send window:             ${wallClockSeconds.toFixed(1)}s`);
  console.log(`First send -> last row:  ${durableSeconds.toFixed(1)}s (drain after window: ${(durableSeconds - wallClockSeconds).toFixed(1)}s)${drained ? "" : "  ⚠️ lag never reached 0 within 120s"}`);
  console.log(`Accepted rate:           ${acceptedPerSec.toFixed(1)} events/s (persisted / send window — not a sustained durable rate)`);
  console.log(`Durable rate:            ${durablePerSec.toFixed(1)} events/s (persisted / first send -> last durable row)`);
  console.log(
    `End-to-end latency p50/p95/p99 (ms): ${percentile(latencies, 50)} / ${percentile(latencies, 95)} / ${percentile(
      latencies,
      99
    )}`
  );
  console.log("(end-to-end latency = produce -> Redpanda -> consumer batch insert -> durable Postgres row)");
  console.log("================================================\n");
  console.log("Copy the numbers above into README.md once you've run this for real.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
