import { spawnSync, spawn } from "node:child_process";
import crypto from "node:crypto";

export const GATEWAY = process.env.GATEWAY_URL || "http://localhost:3000";
export const API_KEY = "ps_live_test_key_abc123xyz";
export const C = {
  gateway: "pulsestream-gateway",
  consumer: "pulsestream-consumer",
  postgres: "pulsestream-postgres",
  redis: "pulsestream-redis",
  redpanda: "pulsestream-redpanda",
};

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const uuid = () => crypto.randomUUID();

function run(cmd, args, { input, timeout = 60_000, allowFail = false } = {}) {
  const r = spawnSync(cmd, args, { input, encoding: "utf8", timeout, maxBuffer: 64 * 1024 * 1024 });
  if (!allowFail && r.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} failed (${r.status}): ${r.stderr || r.error}`);
  }
  return r;
}

export const docker = (...args) => run("docker", args).stdout.trim();
export const dockerTry = (...args) => run("docker", args, { allowFail: true });

// SQL goes over stdin, not argv, so large ID lists can't hit argv limits.
export function psql(sql) {
  return run("docker", ["exec", "-i", C.postgres, "psql", "-U", "postgres", "-d", "pulsestream", "-t", "-A", "-F", "|"], {
    input: sql,
  }).stdout.trim();
}

export const redisCli = (...args) => run("docker", ["exec", C.redis, "redis-cli", ...args]).stdout.trim();
export const rpk = (...args) => run("docker", ["exec", C.redpanda, "rpk", ...args], { allowFail: true });

export async function waitFor(fn, { timeout = 60_000, interval = 500, what = "condition" } = {}) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeout) {
    try {
      last = await fn();
      if (last) return last;
    } catch (e) {
      last = e;
    }
    await sleep(interval);
  }
  throw new Error(`Timed out after ${timeout}ms waiting for ${what} (last: ${last?.message ?? last})`);
}

export function newEvent(overrides = {}) {
  return {
    deviceId: uuid(),
    eventType: "temperature",
    value: Math.round(Math.random() * 10_000) / 100,
    timestamp: Date.now(),
    ...overrides,
  };
}

export async function postEvent(key, body, { timeoutMs = 15_000 } = {}) {
  const started = Date.now();
  try {
    const res = await fetch(`${GATEWAY}/v1/events`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": API_KEY, "Idempotency-Key": key },
      body: typeof body === "string" ? body : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      json = text;
    }
    return { status: res.status, json, ms: Date.now() - started };
  } catch (e) {
    return { status: 0, error: e.name, ms: Date.now() - started };
  }
}

// ---- persistence / DLQ / lag observation -----------------------------------

export function fetchRows(ids) {
  if (ids.length === 0) return [];
  const arr = `ARRAY[${ids.map((i) => `'${i}'`).join(",")}]::uuid[]`;
  const out = psql(`SELECT id, device_id, event_type, value, timestamp FROM events WHERE id = ANY(${arr});`);
  return out
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      const [id, device_id, event_type, value, timestamp] = l.split("|");
      return { id, device_id, event_type, value: Number(value), timestamp: Number(timestamp) };
    });
}

export function countRows(id) {
  return Number(psql(`SELECT count(*) FROM events WHERE id = '${id}';`));
}

// Every value on metrics.dlq, parsed: a snapshot up to the current high
// watermark (rpk has no portable "read to end and exit" flag, and an
// open-ended consume would block).
export function dlqContents() {
  const desc = rpk("topic", "describe", "metrics.dlq", "-a").stdout || "";
  const row = desc.split("\n").map((l) => l.trim().split(/\s+/)).find((c) => /^\d+$/.test(c[0]) && c.length >= 6);
  const hwm = row ? Number(row[row.length - 1]) : 0;
  if (!hwm) return [];
  const r = rpk("topic", "consume", "metrics.dlq", "-o", "start", "-n", String(hwm), "-f", "%v\\n");
  return (r.stdout || "")
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return { raw: l };
      }
    });
}

export function groupState() {
  const out = rpk("group", "describe", "pulsestream-metrics-group").stdout || "";
  const state = /STATE\s+(\S+)/.exec(out)?.[1];
  let lag = 0;
  let rows = 0;
  for (const l of out.split("\n")) {
    if (l.trim().startsWith("metrics.raw")) {
      rows++;
      lag += Number(l.trim().split(/\s+/)[4]) || 0;
    }
  }
  return { state, lag, rows };
}

export async function waitForDrain(timeout = 150_000) {
  return waitFor(
    () => {
      const g = groupState();
      return g.state === "Stable" && g.rows > 0 && g.lag === 0;
    },
    { timeout, interval: 1000, what: "consumer group Stable with lag 0" },
  );
}

export async function waitForGateway(timeout = 120_000) {
  return waitFor(async () => (await fetch(`${GATEWAY}/health`)).ok, { timeout, interval: 1000, what: "gateway /health" });
}

// ---- fault injection --------------------------------------------------------

export const pause = (c) => docker("pause", c);
export const unpause = (c) => dockerTry("unpause", c);
export const kill9 = (c) => docker("kill", "--signal=SIGKILL", c);
export const start = (c) => docker("start", c);
export const stop = (c) => docker("stop", "-t", "2", c);

export async function restartConsumerAfterCrash() {
  dockerTry("kill", "--signal=SIGKILL", C.consumer);
  start(C.consumer);
}

// Holds an uncommitted INSERT of `row` open, so a consumer inserting the same
// id blocks on the unique index mid-transaction. Deterministic way to freeze
// the consumer between BEGIN and COMMIT.
export async function holdUncommittedInsert(id) {
  const child = spawn("docker", ["exec", "-i", C.postgres, "psql", "-U", "postgres", "-d", "pulsestream", "-q"], {
    stdio: ["pipe", "ignore", "ignore"],
  });
  child.stdin.write(
    `BEGIN;\nINSERT INTO events (id, device_id, event_type, value, timestamp) VALUES ('${id}', '${uuid()}', 'placeholder', 0, 1);\n`,
  );
  // The consumer must not insert this id before we hold it, or there is no block.
  await waitFor(
    () => Number(psql(`SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction' AND query ILIKE '%${id}%';`)) > 0,
    { timeout: 30_000, interval: 200, what: "blocker transaction to hold the row" },
  );
  return {
    release: async () => {
      child.stdin.write("ROLLBACK;\n\\q\n");
      child.stdin.end();
      await new Promise((r) => child.on("close", r));
    },
  };
}

export function terminateOpenTransactions() {
  psql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND pid <> pg_backend_pid();");
}

export function consumerBlockedOnLock() {
  return Number(
    psql(
      `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE 'INSERT INTO events%';`,
    ),
  ) > 0;
}

// ---- ledger: acknowledged vs persisted vs payload ---------------------------

export class Ledger {
  constructor() {
    this.acked = new Map(); // idempotency key -> submitted payload
  }
  async submit(key, body, opts) {
    const res = await postEvent(key, body, opts);
    if (res.status === 202) this.acked.set(key, typeof body === "string" ? JSON.parse(body) : body);
    return res;
  }
  /** Every acknowledged event must be stored exactly once with the payload that was submitted. */
  verify() {
    const ids = [...this.acked.keys()];
    const rows = fetchRows(ids);
    const byId = new Map(rows.map((r) => [r.id, r]));
    const lost = ids.filter((id) => !byId.has(id));
    const mismatched = [];
    for (const [id, p] of this.acked) {
      const r = byId.get(id);
      if (!r) continue;
      if (r.device_id !== p.deviceId || r.event_type !== p.eventType || r.value !== p.value || r.timestamp !== p.timestamp) {
        mismatched.push({ id, submitted: p, stored: r });
      }
    }
    const dupes = ids.filter((id) => countRows(id) > 1);
    return { acked: ids.length, persisted: rows.length, lost, mismatched, dupes };
  }
  async waitUntilPersisted(timeout = 150_000) {
    await waitFor(() => fetchRows([...this.acked.keys()]).length === this.acked.size, {
      timeout,
      interval: 1000,
      what: `all ${this.acked.size} acked events persisted`,
    });
  }
}
