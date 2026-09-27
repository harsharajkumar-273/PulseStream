#!/usr/bin/env node
/**
 * Failure-injection test for the ingestion pipeline.
 *
 * Produces a batch of uniquely-identified events directly onto the
 * `metrics.raw` Kafka topic (bypassing the HTTP gateway so the test
 * controls timing precisely), while concurrently killing the consumer
 * and/or Postgres containers mid-stream via `docker kill` + `docker start`
 * (a SIGKILL, not a graceful shutdown). Once the consumer has drained,
 * it compares the set of event ids actually stored in Postgres against
 * the set of ids sent, and reports:
 *
 *   - lost:      sent but never stored (data loss)
 *   - duplicate: stored more than once (should be impossible: `id` is the
 *                table's primary key and inserts use ON CONFLICT DO NOTHING,
 *                so this count is a correctness assertion, not a measurement)
 *
 * Requires the stack to already be running: `docker-compose up --build -d`.
 *
 * Usage:
 *   node benchmarks/crash_test.js [--count 5000] [--kills 3]
 */
import { Kafka } from "kafkajs";
import crypto from "crypto";
import { execFileSync, spawnSync } from "child_process";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const TOTAL = Number(arg("count", 5000));
const NUM_KILLS = Number(arg("kills", 3));
const CONSUMER_CONTAINER = "pulsestream-consumer";
const POSTGRES_CONTAINER = "pulsestream-postgres";
const TOPIC = "metrics.raw";

// Queries are piped via stdin (not `-c <arg>`) since the id-matching query
// below can easily exceed OS argv length limits at a few thousand events.
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

function dockerKill(container) {
  execFileSync("docker", ["kill", "--signal=SIGKILL", container], { stdio: "ignore" });
}

function dockerStart(container) {
  execFileSync("docker", ["start", container], { stdio: "ignore" });
}

function isRunning(container) {
  const out = execFileSync("docker", ["inspect", "-f", "{{.State.Running}}", container], {
    encoding: "utf8",
  }).trim();
  return out === "true";
}

// Postgres row-count stability alone is a false-positive trap: right after a
// SIGKILL, the consumer container is "running" again long before it has
// rejoined the Kafka consumer group (rebalance can take 10s of seconds), so
// row count looks "stable at 0" for several polls even though the consumer
// simply hasn't resumed yet. Group lag is the real signal that it has caught up.
function consumerLag() {
  let out;
  try {
    out = execFileSync("docker", ["exec", "pulsestream-redpanda", "rpk", "group", "describe", "pulsestream-metrics-group"], {
      encoding: "utf8",
    });
  } catch {
    return null; // group not up yet (e.g. broker mid-restart)
  }
  const line = out.split("\n").find((l) => l.trim().startsWith("metrics.raw"));
  if (!line) return null;
  const cols = line.trim().split(/\s+/);
  const lag = Number(cols[4]); // TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
  return Number.isFinite(lag) ? lag : null;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function injectFailures() {
  // Alternate which container gets killed, spaced out over the send window,
  // so the test covers "consumer dies mid-batch" and "DB dies mid-transaction".
  const targets = [];
  for (let i = 0; i < NUM_KILLS; i++) {
    targets.push(i % 2 === 0 ? CONSUMER_CONTAINER : POSTGRES_CONTAINER);
  }

  const log = [];
  for (const target of targets) {
    await sleep(1500 + Math.random() * 2000);
    console.log(`💥 Killing ${target} (SIGKILL)...`);
    dockerKill(target);
    log.push({ target, killedAt: Date.now() });
    // Leave it down briefly to guarantee in-flight batches/transactions are
    // actually interrupted, then bring it back like a real crash-restart.
    await sleep(800 + Math.random() * 1200);
    dockerStart(target);
    console.log(`♻️  Restarted ${target}.`);
  }
  return log;
}

async function produceEvents() {
  const kafka = new Kafka({ clientId: "crash-test-producer", brokers: ["localhost:9092"] });
  const producer = kafka.producer();
  await producer.connect();

  const sentIds = new Set();
  const BATCH_SIZE = 200;

  for (let sent = 0; sent < TOTAL; sent += BATCH_SIZE) {
    const n = Math.min(BATCH_SIZE, TOTAL - sent);
    const messages = [];
    for (let i = 0; i < n; i++) {
      const id = crypto.randomUUID();
      sentIds.add(id);
      messages.push({
        key: `device-${(sent + i) % 50}`,
        value: JSON.stringify({
          id,
          deviceId: crypto.randomUUID(),
          eventType: "temperature",
          value: Math.random() * 100,
          timestamp: Date.now(),
        }),
      });
    }
    await producer.send({ topic: TOPIC, messages }).catch((err) => {
      // Broker may be mid-restart; the test cares about durability of what's
      // acknowledged, so a produce error here just means fewer messages sent.
      console.warn(`⚠️  Produce error (continuing): ${err.message}`);
    });
    await sleep(50);
  }

  await producer.disconnect();
  return sentIds;
}

async function waitForDrain() {
  // Wait for the Kafka consumer group to report zero lag (the consumer has
  // actually rejoined and caught up), not just for Postgres row counts to
  // stop moving — a container can be "running" again well before its
  // consumer group rebalance finishes.
  const start = Date.now();
  let lagZeroStreak = 0;
  while (Date.now() - start < 180_000) {
    if (!isRunning(CONSUMER_CONTAINER) || !isRunning(POSTGRES_CONTAINER)) {
      await sleep(1000);
      continue;
    }
    const lag = consumerLag();
    if (lag === 0) {
      lagZeroStreak++;
      if (lagZeroStreak >= 3) break;
    } else {
      lagZeroStreak = 0;
    }
    await sleep(1500);
  }
  return Number(psql("SELECT count(*) FROM events;"));
}

async function main() {
  console.log(`🚀 Sending ${TOTAL} events to '${TOPIC}' with ${NUM_KILLS} injected crashes...\n`);

  const before = Number(psql("SELECT count(*) FROM events;"));

  const [sentIds] = await Promise.all([produceEvents(), injectFailures()]);

  console.log("\n⏳ Waiting for consumer to drain and stabilize...");
  await waitForDrain();

  // Chunked and via stdin: a single query embedding many thousands of UUIDs
  // can overflow the child process's stdin pipe buffer (ENOBUFS) otherwise.
  const CHUNK = 2000;
  const allIds = [...sentIds];
  const lost = [];
  let duplicateCheck = 0;
  for (let i = 0; i < allIds.length; i += CHUNK) {
    const chunk = allIds.slice(i, i + CHUNK);
    const arrLiteral = `ARRAY[${chunk.map((id) => `'${id}'`).join(",")}]::uuid[]`;

    // ids sent but never landed a row
    lost.push(
      ...psql(
        `WITH sent(id) AS (SELECT unnest(${arrLiteral}))
         SELECT sent.id FROM sent LEFT JOIN events e ON e.id = sent.id WHERE e.id IS NULL;`
      )
        .split("\n")
        .filter(Boolean)
    );

    // Structurally, this should always be 0: `events.id` is the primary key
    // and inserts use ON CONFLICT DO NOTHING, so Postgres itself forbids a
    // second row for the same id. Correctness assertion, not a measurement.
    duplicateCheck += Number(
      psql(
        `SELECT count(*) FROM (SELECT id FROM events WHERE id = ANY(${arrLiteral}) GROUP BY id HAVING count(*) > 1) d;`
      )
    );
  }

  const after = Number(psql("SELECT count(*) FROM events;"));
  const storedCount = sentIds.size - lost.length;

  console.log("\n=== Crash-injection test result ===");
  console.log(`Sent:              ${sentIds.size}`);
  console.log(`Stored (matched):  ${storedCount}`);
  console.log(`Lost:              ${lost.length}`);
  console.log(`Duplicated:        ${duplicateCheck}`);
  console.log(`Injected crashes:  ${NUM_KILLS}`);
  console.log(`events table rows: ${before} -> ${after}`);
  console.log("====================================\n");

  if (lost.length > 0) {
    console.log(`❌ FAIL: ${lost.length} events lost across ${NUM_KILLS} injected failures.`);
    console.log("First few lost ids:", lost.slice(0, 10));
    process.exit(1);
  }
  if (duplicateCheck > 0) {
    console.log(`❌ FAIL: ${duplicateCheck} events duplicated (should be impossible with PK + ON CONFLICT).`);
    process.exit(1);
  }
  console.log(`✅ PASS: 0 lost / 0 duplicated across ${NUM_KILLS} injected crashes.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
