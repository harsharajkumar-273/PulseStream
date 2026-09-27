#!/usr/bin/env node
/**
 * Consumer-lag-under-a-traffic-spike chart, and a scale-out comparison.
 *
 * `metrics.raw` has 3 partitions (see keda-hpa.yaml / KEDA's kafka trigger,
 * which scales `metrics-consumer` on partition lag), so a single consumer
 * replica only ever pulls from 3 partitions serially through one process,
 * while N replicas in the same consumer group each get their own partition
 * share. This script sends one large burst of events (a traffic spike),
 * polls total consumer-group lag every second until it drains to 0, and
 * prints a simple text timeline — first with 1 replica, then again with
 * `--replicas` (default 3) to show KEDA's actual lever: more replicas ->
 * more partitions consumed in parallel -> lag drains faster.
 *
 * This runs against the docker-compose stack directly (`docker-compose up
 * -d --scale metrics-consumer=N`) as a stand-in for real KEDA, which scales
 * Kubernetes pods on this exact same signal (see cluster/ for the real thing).
 *
 * Requires the stack to already be running: `docker-compose up --build -d`.
 *
 * Usage:
 *   node benchmarks/lag_chart.js [--count 20000] [--replicas 3]
 */
import { Kafka } from "kafkajs";
import crypto from "crypto";
import { execFileSync, spawnSync } from "child_process";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : process.argv[i + 1];
}

const TOTAL = Number(arg("count", 20000));
const REPLICAS = Number(arg("replicas", 3));
const TOPIC = "metrics.raw";
const GROUP = "pulsestream-metrics-group";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// docker-compose's own `metrics-consumer` service pins a fixed
// `container_name` and host port, which blocks `docker-compose --scale`
// outright. Rather than touch that shared config (other scripts hard-code
// the "pulsestream-consumer" container name), extra replicas here are
// plain `docker run` containers on the same network/image, standing in for
// what would be additional Kubernetes pods under real KEDA.
const NETWORK = "pulsestream_default";
const IMAGE = "pulsestream-metrics-consumer";
// Replica 1 is always "pulsestream-consumer", owned by docker-compose and
// left alone; this only ever adds/removes replicas 2..N.

function extraReplicaName(i) {
  return `pulsestream-consumer-extra-${i}`;
}

function containerExists(name) {
  const res = spawnSync("docker", ["inspect", name], { stdio: "ignore" });
  return res.status === 0;
}

function scaleConsumer(n) {
  const extrasWanted = Math.max(0, n - 1);
  for (let i = 1; i <= 10; i++) {
    const name = extraReplicaName(i);
    if (i <= extrasWanted) {
      if (!containerExists(name)) {
        execFileSync(
          "docker",
          [
            "run",
            "-d",
            "--name",
            name,
            "--network",
            NETWORK,
            "-e",
            "NODE_ENV=development",
            "-e",
            "REDIS_URL=redis://redis:6379",
            "-e",
            "DATABASE_URL=postgresql://postgres:postgres@postgres:5432/pulsestream",
            "-e",
            "KAFKA_BROKERS=redpanda:29092",
            "-e",
            "CONSUMER_PORT=3001",
            IMAGE,
            "node",
            "dist/consumer.js",
          ],
          { stdio: "ignore" }
        );
      }
    } else if (containerExists(name)) {
      execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" });
    }
  }
}

function totalLag() {
  const out = spawnSync("docker", ["exec", "pulsestream-redpanda", "rpk", "group", "describe", GROUP], {
    encoding: "utf8",
  }).stdout;
  let sum = 0;
  let sawRow = false;
  for (const line of (out || "").split("\n")) {
    const t = line.trim();
    if (t.startsWith("metrics.raw")) {
      const cols = t.split(/\s+/);
      const lag = Number(cols[4]);
      if (Number.isFinite(lag)) {
        sum += lag;
        sawRow = true;
      }
    }
  }
  return sawRow ? sum : null;
}

async function sendBurst() {
  const kafka = new Kafka({ clientId: "lag-chart-producer", brokers: ["localhost:9092"] });
  const producer = kafka.producer();
  await producer.connect();

  const BATCH_SIZE = 500;
  for (let sent = 0; sent < TOTAL; sent += BATCH_SIZE) {
    const n = Math.min(BATCH_SIZE, TOTAL - sent);
    const messages = [];
    for (let i = 0; i < n; i++) {
      messages.push({
        key: `device-${(sent + i) % 300}`, // spread across all 3 partitions
        value: JSON.stringify({
          id: crypto.randomUUID(),
          deviceId: crypto.randomUUID(),
          eventType: "temperature",
          value: Math.random() * 100,
          timestamp: Date.now(),
        }),
      });
    }
    await producer.send({ topic: TOPIC, messages });
  }
  await producer.disconnect();
}

function renderTimeline(label, samples) {
  console.log(`\n--- ${label} ---`);
  console.log("t(s)  lag");
  const peak = Math.max(...samples.map((s) => s.lag));
  for (const s of samples) {
    const bar = "#".repeat(Math.min(80, Math.round((s.lag / Math.max(1, peak)) * 80)));
    const marker = s.marker ? `  <-- ${s.marker}` : "";
    console.log(`${String(s.t).padStart(4)}  ${String(s.lag).padStart(6)}  ${bar}${marker}`);
  }
  const drainedAt = samples.find((s) => s.lag === 0 && !s.marker);
  console.log(`Drain time: ${drainedAt ? drainedAt.t + "s" : "did not reach 0 within window"}`);
}

async function main() {
  console.log("=== Consumer lag under a traffic spike, with a mid-drain scale-out ===");
  console.log(`Sending one ${TOTAL}-event burst, starting at 1 replica, scaling to ${REPLICAS} partway through the drain.\n`);

  scaleConsumer(1);
  await sleep(4000);

  const t0 = Date.now();
  await sendBurst();
  console.log(`Burst of ${TOTAL} sent in ${((Date.now() - t0) / 1000).toFixed(1)}s. Polling lag every 1s...`);

  const SCALE_OUT_AT_S = Number(arg("scale-out-at", 15));
  const samples = [];
  const start = Date.now();
  let zeroStreak = 0;
  let scaledOut = false;

  while (Date.now() - start < 300_000) {
    const lag = totalLag();
    const t = Math.round((Date.now() - start) / 1000);

    if (!scaledOut && t >= SCALE_OUT_AT_S) {
      scaleConsumer(REPLICAS);
      scaledOut = true;
      samples.push({ t, lag: lag ?? samples[samples.length - 1]?.lag ?? 0, marker: `-> scaled to ${REPLICAS} replicas` });
      console.log(`\n[t=${t}s] Scaling out to ${REPLICAS} replicas (this is what KEDA's kafka trigger would do here)...`);
    }

    if (lag !== null) {
      samples.push({ t, lag });
      if (lag === 0) {
        zeroStreak++;
        if (zeroStreak >= 3) break;
      } else {
        zeroStreak = 0;
      }
    }
    await sleep(1000);
  }

  renderTimeline(`1 replica -> ${REPLICAS} replicas at t=${SCALE_OUT_AT_S}s`, samples);

  console.log("\nScaling back down to 1 replica (KEDA's cooldown behavior).");
  scaleConsumer(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
