#!/usr/bin/env node
/**
 * Real load-test script for PulseStream's /ingest endpoint.
 * Run this against a live `docker-compose up` stack to produce real
 * throughput/latency numbers instead of narrated ones.
 *
 * Usage:
 *   npm install --save-dev autocannon
 *   node benchmarks/load_test.js [--url http://localhost:3000/ingest] [--duration 30] [--connections 50]
 *
 * Prints p50/p95/p99 latency and requests/sec to stdout. Paste the
 * output into README.md's benchmark section once you've run it for
 * real, so the numbers there are backed by this exact command.
 */
const autocannon = require("autocannon");

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const url = arg("url", "http://localhost:3000/ingest");
const duration = Number(arg("duration", 30));
const connections = Number(arg("connections", 50));

function samplePayload() {
  return JSON.stringify({
    deviceId: `sensor-${Math.floor(Math.random() * 1000)}`,
    metric: "temperature",
    value: Math.random() * 100,
    timestamp: Date.now(),
  });
}

const instance = autocannon(
  {
    url,
    connections,
    duration,
    method: "POST",
    headers: { "content-type": "application/json" },
    body: samplePayload(),
    setupClient: (client) => {
      client.setBody(samplePayload());
    },
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
    console.log(`Latency p50/p95/p99 (ms): ${result.latency.p50} / ${result.latency.p95} / ${result.latency.p99}`);
    console.log(`2xx responses: ${result["2xx"]}, non-2xx/errors: ${result.non2xx + result.errors}`);
    console.log("=====================================\n");
    console.log("Copy the numbers above into README.md once you've run this for real.");
  }
);

autocannon.track(instance, { renderProgressBar: true });
