/**
 * API-level failure-semantics suite. Every event goes in through POST /v1/events
 * (auth, idempotency, validation, Kafka publish, consumer, Postgres) -- unlike
 * benchmarks/crash_test.js, which produces straight onto Kafka.
 *
 * For each scenario the Ledger tracks acknowledged ids, persisted ids and
 * payload correctness; DLQ contents are read back from metrics.dlq.
 *
 * Needs the compose stack: postgres redis redpanda create-topic api-gateway metrics-consumer.
 */
import { describe, it, before, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  C, Ledger, uuid, newEvent, postEvent, sleep, waitFor, waitForDrain, waitForGateway,
  fetchRows, countRows, dlqContents, groupState, redisCli, rpk, psql,
  pause, unpause, kill9, start, stop, dockerTry, holdUncommittedInsert, consumerBlockedOnLock, terminateOpenTransactions,
} from "./helpers.mjs";

const SLOW = { timeout: 300_000 };

async function ensureHealthy() {
  terminateOpenTransactions(); // a failed test must not leave the consumer blocked on a stray lock
  for (const c of [C.consumer, C.redis, C.redpanda, C.postgres, C.gateway]) {
    dockerTry("unpause", c);
    const running = dockerTry("inspect", "-f", "{{.State.Running}}", c).stdout.trim() === "true";
    if (!running) start(c);
  }
  rpk("topic", "alter-config", "metrics.dlq", "--delete", "max.message.bytes");
  await waitForGateway();
  await waitForDrain();
}

// NUL in a string passes the gateway's Zod schema (length 2..50) but Postgres
// rejects it, so this is a poison message that is reachable through the API.
const poison = (deviceId) => newEvent({ deviceId, eventType: "ab\u0000cd" });

describe("API failure semantics", () => {
  before(async () => {
    await waitForGateway();
    rpk("topic", "create", "metrics.dlq");
    await waitForDrain();
  });
  beforeEach(ensureHealthy);
  afterEach(ensureHealthy);

  it("happy path: every acknowledged event is stored once with the submitted payload", SLOW, async () => {
    const ledger = new Ledger();
    for (let i = 0; i < 25; i++) {
      const res = await ledger.submit(uuid(), newEvent());
      assert.equal(res.status, 202);
    }
    await ledger.waitUntilPersisted();
    const v = ledger.verify();
    assert.deepEqual({ lost: v.lost, mismatched: v.mismatched, dupes: v.dupes }, { lost: [], mismatched: [], dupes: [] });
  });

  it("valid-invalid-valid batch: valid events persist, the invalid one reaches the DLQ", SLOW, async () => {
    const deviceId = uuid(); // same key -> same partition -> same consumer batch
    const ledger = new Ledger();
    const badKey = uuid();

    pause(C.consumer); // let all three accumulate so they arrive as one batch
    const r1 = await ledger.submit(uuid(), newEvent({ deviceId }));
    const rBad = await postEvent(badKey, poison(deviceId));
    const r2 = await ledger.submit(uuid(), newEvent({ deviceId }));
    assert.deepEqual([r1.status, rBad.status, r2.status], [202, 202, 202], "API acknowledges all three");
    unpause(C.consumer);

    await ledger.waitUntilPersisted(120_000);
    await waitFor(() => dlqContents().some((m) => m.id === badKey), { timeout: 60_000, what: "poison event on DLQ" });

    const v = ledger.verify();
    assert.deepEqual({ lost: v.lost, mismatched: v.mismatched, dupes: v.dupes }, { lost: [], mismatched: [], dupes: [] });
    assert.equal(countRows(badKey), 0, "poison event must not be stored");
    const dlq = dlqContents();
    for (const goodKey of ledger.acked.keys()) {
      assert.ok(!dlq.some((m) => m.id === goodKey), `valid event ${goodKey} must not be dead-lettered`);
    }
    assert.equal(dlq.filter((m) => m.id === badKey).length, 1, "poison event dead-lettered exactly once");
  });

  it("same idempotency key, repeated sequentially: one stored event, consistent response", SLOW, async () => {
    const key = uuid();
    const body = newEvent();
    const ledger = new Ledger();
    const responses = [];
    for (let i = 0; i < 5; i++) responses.push(await ledger.submit(key, body));
    assert.ok(responses.every((r) => r.status === 202));
    assert.ok(responses.every((r) => JSON.stringify(r.json) === JSON.stringify(responses[0].json)));
    await ledger.waitUntilPersisted();
    await sleep(2000);
    assert.equal(countRows(key), 1);
  });

  it("same idempotency key, concurrent: one stored event, no divergent responses", SLOW, async () => {
    const key = uuid();
    const body = newEvent();
    const ledger = new Ledger();
    const responses = await Promise.all(Array.from({ length: 8 }, () => ledger.submit(key, body)));
    assert.ok(responses.every((r) => r.status === 202 || r.status === 409), JSON.stringify(responses.map((r) => r.status)));
    const accepted = responses.filter((r) => r.status === 202);
    assert.ok(accepted.length >= 1);
    assert.ok(accepted.every((r) => JSON.stringify(r.json) === JSON.stringify(accepted[0].json)));
    await ledger.waitUntilPersisted();
    await sleep(2000);
    assert.equal(countRows(key), 1);
  });

  it("same key, different payload: explicit conflict, first payload wins", SLOW, async () => {
    const key = uuid();
    const first = newEvent({ value: 1.5 });
    const second = { ...first, value: 99.9 };
    const ledger = new Ledger();

    assert.equal((await ledger.submit(key, first)).status, 202);
    const conflict = await postEvent(key, second);
    assert.equal(conflict.status, 422, `expected an explicit conflict, got ${conflict.status} ${JSON.stringify(conflict.json)}`);

    // Key order must not matter: same logical payload is a plain replay.
    const reordered = Object.fromEntries(Object.entries(first).reverse());
    assert.equal((await postEvent(key, reordered)).status, 202);

    await ledger.waitUntilPersisted();
    await sleep(2000);
    assert.equal(countRows(key), 1);
    const v = ledger.verify();
    assert.deepEqual(v.mismatched, [], "stored payload must be the first one submitted");
  });

  it("consumer SIGKILLed mid-transaction (before COMMIT): nothing lost, nothing duplicated", SLOW, async () => {
    const deviceId = uuid();
    const ledger = new Ledger();
    const blockerKey = uuid();
    const holder = await holdUncommittedInsert(blockerKey); // consumer will block on this id
    try {
      await ledger.submit(uuid(), newEvent({ deviceId }));
      assert.equal((await ledger.submit(blockerKey, newEvent({ deviceId }))).status, 202);
      await ledger.submit(uuid(), newEvent({ deviceId }));

      await waitFor(() => consumerBlockedOnLock(), { timeout: 60_000, what: "consumer blocked inside its transaction" });
      kill9(C.consumer);
    } finally {
      await holder.release(); // rolls back the placeholder row
    }
    start(C.consumer);

    await ledger.waitUntilPersisted();
    const v = ledger.verify();
    assert.deepEqual({ lost: v.lost, mismatched: v.mismatched, dupes: v.dupes }, { lost: [], mismatched: [], dupes: [] });
  });

  it("failure after DB COMMIT but before offset advancement, then SIGKILL: stored once, poison dead-lettered", SLOW, async () => {
    // Make the post-commit DLQ send fail. The batch's rows are already
    // committed at that point, but the offsets must not advance.
    rpk("topic", "alter-config", "metrics.dlq", "--set", "max.message.bytes=10");
    const deviceId = uuid();
    const ledger = new Ledger();
    const badKey = uuid();

    pause(C.consumer);
    await ledger.submit(uuid(), newEvent({ deviceId }));
    assert.equal((await postEvent(badKey, poison(deviceId))).status, 202);
    await ledger.submit(uuid(), newEvent({ deviceId }));
    unpause(C.consumer);

    await ledger.waitUntilPersisted(90_000); // rows committed...
    assert.ok(groupState().lag > 0, "...but offsets must not have advanced past the failed batch");

    kill9(C.consumer); // crash while the batch is pending redelivery
    rpk("topic", "alter-config", "metrics.dlq", "--delete", "max.message.bytes");
    start(C.consumer);

    await waitForDrain();
    await waitFor(() => dlqContents().some((m) => m.id === badKey), { timeout: 60_000, what: "poison event on DLQ after recovery" });
    const v = ledger.verify();
    assert.deepEqual({ lost: v.lost, mismatched: v.mismatched, dupes: v.dupes }, { lost: [], mismatched: [], dupes: [] });
    assert.equal(countRows(badKey), 0);
    // DLQ delivery is at-least-once: redelivery may dead-letter the poison event more than once.
    assert.ok(dlqContents().filter((m) => m.id === badKey).length >= 1);
  });

  it("Redis outage: requests fail fast and are never acknowledged; service recovers; replays are deduplicated", SLOW, async () => {
    const ledger = new Ledger();
    const before = uuid();
    assert.equal((await ledger.submit(before, newEvent())).status, 202);

    stop(C.redis);
    const outageKey = uuid();
    const outageBody = newEvent();
    const down = await postEvent(outageKey, outageBody, { timeoutMs: 15_000 });
    assert.ok(down.status >= 500, `expected a 5xx while Redis is down, got ${down.status} (${down.error ?? ""})`);
    assert.ok(down.ms < 10_000, `expected a fast failure, took ${down.ms}ms`);

    start(C.redis);
    // Client retries the same key until the gateway has recovered.
    await waitFor(async () => (await ledger.submit(outageKey, outageBody)).status === 202, {
      timeout: 60_000, interval: 1000, what: "gateway to recover after Redis restart",
    });

    // Redis lost all idempotency state: a replay of an already-acknowledged key
    // is accepted again, and Postgres' primary key is the backstop.
    redisCli("FLUSHALL");
    const replayBody = ledger.acked.get(before);
    assert.equal((await postEvent(before, replayBody)).status, 202);

    await ledger.waitUntilPersisted();
    await waitForDrain();
    assert.equal(countRows(before), 1);
    assert.equal(countRows(outageKey), 1);
    const v = ledger.verify();
    assert.deepEqual({ lost: v.lost, mismatched: v.mismatched, dupes: v.dupes }, { lost: [], mismatched: [], dupes: [] });
  });

  it("request outlives its idempotency lock: second publish is absorbed, one row stored", SLOW, async () => {
    const key = uuid();
    const body = newEvent();
    const ledger = new Ledger();

    pause(C.redpanda); // request A now hangs in the Kafka publish while holding the lock
    const a = ledger.submit(key, body, { timeoutMs: 60_000 });
    await sleep(2000);
    redisCli("DEL", `idempotency:key:${key}`); // identical state to the 10s lock TTL expiring
    const b = postEvent(key, body, { timeoutMs: 60_000 }); // acquires a fresh lock, publishes again
    await sleep(1000);
    unpause(C.redpanda);

    const [ra, rb] = await Promise.all([a, b]);
    assert.equal(ra.status, 202);
    assert.equal(rb.status, 202);
    assert.equal(ra.json.data.id, rb.json.data.id);

    await ledger.waitUntilPersisted();
    await waitForDrain();
    assert.equal(countRows(key), 1, "duplicate publish must collapse to a single row");
    assert.deepEqual(ledger.verify().mismatched, []);
  });
});
