import WebSocket from 'ws';
import crypto from 'crypto';

const GATEWAY_URL = 'http://localhost:3000';
const WS_STREAM_URL = 'ws://localhost:3000/v1/stream';
const VALID_API_KEY = 'ps_live_test_key_abc123xyz';

// Statistics counters
const stats = {
  success202: 0,
  conflict409: 0,
  badRequest400: 0,
  unauthorized401: 0,
  unexpectedErrors: 0,
  wsEventsReceived: 0,
};

// Sleep helper
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// 1. Initialize WebSocket Subscriber to watch real-time broadcast stream
const runWebSocketSubscriber = () => {
  console.log('📡 Starting live WebSocket listener...');
  const ws = new WebSocket(WS_STREAM_URL);

  ws.on('open', () => {
    console.log('🔌 WebSocket subscriber connected to live stream');
  });

  ws.on('message', (data) => {
    stats.wsEventsReceived++;
    try {
      const event = JSON.parse(data.toString());
      console.log(
        `📬 [WS Stream Alert] Device: ${event.deviceId.slice(0, 8)}... | Type: ${event.eventType} | Value: ${event.value.toFixed(2)}`
      );
    } catch {
      console.log('📬 [WS Stream Alert] Received raw non-JSON frame:', data.toString());
    }
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket Client Error:', err.message);
  });

  ws.on('close', () => {
    console.log('🛑 WebSocket subscriber connection closed. Reconnecting in 5s...');
    setTimeout(runWebSocketSubscriber, 5000);
  });
};

// Helper function to send ingestion requests
const sendEvent = async (headers: Record<string, string>, body: any): Promise<number> => {
  try {
    const res = await fetch(`${GATEWAY_URL}/v1/events`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...headers,
      },
      body: JSON.stringify(body),
    });

    const status = res.status;
    const data = await res.json().catch(() => ({}));

    if (status === 202) {
      stats.success202++;
    } else if (status === 409) {
      stats.conflict409++;
      console.log(`🛡️ [Deduplication Success] Key ${headers['Idempotency-Key']} rejected with 409 Conflict`);
    } else if (status === 400) {
      stats.badRequest400++;
      console.log(`⚠️ [Validation Rejection] Schema check failed:`, data.errors || data.message);
    } else if (status === 401) {
      stats.unauthorized401++;
      console.log(`🔒 [Auth Rejection] API Key rejected:`, data.message);
    } else {
      stats.unexpectedErrors++;
      console.error(`💥 Unexpected HTTP Status: ${status}`, data);
    }

    return status;
  } catch (err: any) {
    stats.unexpectedErrors++;
    console.error('❌ Network Connection failed:', err.message);
    return 0;
  }
};

const runSimulation = async () => {
  console.log('\n🚀 Starting Simulation Tests...');

  const testDevice1 = crypto.randomUUID();
  const testDevice2 = crypto.randomUUID();

  // Test Case A: Unauthorized Request
  console.log('\n🧪 Test A: Sending request with invalid API Key...');
  await sendEvent(
    { 'x-api-key': 'bad_token_123', 'Idempotency-Key': crypto.randomUUID() },
    { deviceId: testDevice1, eventType: 'temperature', value: 24.5, timestamp: Date.now() }
  );

  // Test Case B: Schema Validation Failure (Out-of-bounds timestamp)
  console.log('\n🧪 Test B: Sending request with stale timestamp (Zod check)...');
  await sendEvent(
    { 'x-api-key': VALID_API_KEY, 'Idempotency-Key': crypto.randomUUID() },
    {
      deviceId: testDevice1,
      eventType: 'temperature',
      value: 24.5,
      timestamp: Date.now() - 10 * 60 * 1000, // 10 minutes ago (limit is 5 mins)
    }
  );

  // Test Case C: Idempotency Key Lock & Retries
  console.log('\n🧪 Test C: Sending concurrent duplicate requests (Idempotency check)...');
  const sharedKey = crypto.randomUUID();
  const payload = { deviceId: testDevice1, eventType: 'cpu_usage', value: 45.2, timestamp: Date.now() };

  // Fire three requests concurrently with the same idempotency key
  await Promise.all([
    sendEvent({ 'x-api-key': VALID_API_KEY, 'Idempotency-Key': sharedKey }, payload),
    sendEvent({ 'x-api-key': VALID_API_KEY, 'Idempotency-Key': sharedKey }, payload),
    sendEvent({ 'x-api-key': VALID_API_KEY, 'Idempotency-Key': sharedKey }, payload),
  ]);

  // Test Case D: Standard High-Frequency Telemetry Ingestion Loop
  console.log('\n🧪 Test D: Commencing mock IoT device traffic flow (20 metrics events)...');
  for (let i = 0; i < 20; i++) {
    const isDevice1 = Math.random() > 0.5;
    await sendEvent(
      { 'x-api-key': VALID_API_KEY, 'Idempotency-Key': crypto.randomUUID() },
      {
        deviceId: isDevice1 ? testDevice1 : testDevice2,
        eventType: Math.random() > 0.5 ? 'temperature' : 'cpu_usage',
        value: isDevice1 ? 22 + Math.random() * 5 : 40 + Math.random() * 30,
        timestamp: Date.now(),
      }
    );
    await delay(300); // Wait 300ms between metrics
  }

  // Allow time for final WS messages to settle
  await delay(1500);

  console.log('\n📊 --- SIMULATION COMPLETED ---');
  console.log(`✅ Accepted events (202):      ${stats.success202}`);
  console.log(`🛡️ Idempotent rejections (409): ${stats.conflict409}`);
  console.log(`⚠️ Validation rejections (400): ${stats.badRequest400}`);
  console.log(`🔒 Authentication errors (401):  ${stats.unauthorized401}`);
  console.log(`💥 Network/Unexpected errors:   ${stats.unexpectedErrors}`);
  console.log(`📬 Live WS Broadcasts received: ${stats.wsEventsReceived}`);
  console.log('-------------------------------\n');
  
  process.exit(0);
};

// Run the script
runWebSocketSubscriber();
// Wait 1 second for WebSocket handshake to finish before starting simulation
setTimeout(runSimulation, 1000);
