#!/usr/bin/env node
/**
 * Real load-test script for PulseStream's POST /v1/events ingestion endpoint.
 * Run this against a live `docker-compose up` stack to produce real
 * throughput/latency numbers instead of narrated ones.
 *
 * Usage:
 *   npm install --save-dev autocannon
 *   node benchmarks/load_test.js [--url http://localhost:3000/v1/events] [--duration 30] [--connections 50]
 *
 * Requires the seeded dev client from schema.sql (api_key
 * 'ps_live_test_key_abc123xyz') to be present in Postgres — it is inserted
 * automatically the first time the postgres container initializes its volume.
 *
 * Prints p50/p95/p99 latency and requests/sec to stdout. Paste the
 * output into README.md's benchmark section once you've run it for
 * real, so the numbers there are backed by this exact command.
 */
import autocannon from "autocannon";
import crypto from "crypto";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const url = arg("url", "http://localhost:3000/v1/events");
const duration = Number(arg("duration", 30));
const connections = Number(arg("connections", 50));
const apiKey = arg("api-key", "ps_live_test_key_abc123xyz");

// Each request needs its own Idempotency-Key and a fresh timestamp (the
// server rejects timestamps more than 5 minutes old/new), so the body and
// headers are regenerated per request rather than reused across the run.
function setupRequest(request) {
  request.body = JSON.stringify({
    deviceId: crypto.randomUUID(),
    eventType: "temperature",
    value: Math.random() * 100,
    timestamp: Date.now(),
  });
  request.headers["Idempotency-Key"] = crypto.randomUUID();
  return request;
}

const instance = autocannon(
  {
    url,
    connections,
    duration,
    requests: [
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": apiKey,
        },
        setupRequest,
      },
    ],
  },
  (err, result) => {
    if (err) {
      console.error("Load test failed:", err);
      process.exit(1);
    }
    console.log("\n=== PulseStream load test result ===");
    console.log(`Target: ${url}`);
    console.log(`Duration: ${duration}s, Connections: ${connections}`);
    console.log(`Requests/sec (avg): ${result.requests.average}`);
    console.log(`Latency p50/p97.5/p99 (ms): ${result.latency.p50} / ${result.latency.p97_5} / ${result.latency.p99}`);
    console.log(`2xx responses: ${result["2xx"]}, non-2xx/errors: ${result.non2xx + result.errors}`);
    console.log("=====================================\n");
    console.log("Copy the numbers above into README.md once you've run this for real.");
  }
);

autocannon.track(instance, { renderProgressBar: true });
