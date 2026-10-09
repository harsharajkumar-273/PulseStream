import http from 'http';
import pg from 'pg';
import { kafka } from './config/kafka.js';
import db from './config/db.js';
import client from 'prom-client';

const KAFKA_TOPIC = 'metrics.raw';
const DLQ_TOPIC = 'metrics.dlq';
const PORT = process.env.CONSUMER_PORT || '3001';

// Setup Prometheus metrics collection
const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry });

const eventsProcessedCounter = new client.Counter({
  name: 'pulsestream_consumer_events_processed_total',
  help: 'Total number of successfully processed events saved to DB',
  registers: [registry],
});

const eventsFailedCounter = new client.Counter({
  name: 'pulsestream_consumer_events_failed_total',
  help: 'Total number of events that failed processing and routed to DLQ',
  registers: [registry],
});

const dbWriteDuration = new client.Histogram({
  name: 'pulsestream_consumer_db_write_duration_seconds',
  help: 'Histogram of database batch write times',
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5],
  registers: [registry],
});

// Configure Kafka consumer and producer for DLQ
const consumer = kafka.consumer({ groupId: 'pulsestream-metrics-group' });
const dlqProducer = kafka.producer();

// KafkaJS stops the consumer for good on a non-retriable error (e.g. a failed
// DLQ send) while this process and its /health endpoint stay up, which is a
// silent outage. Exit so the orchestrator restarts us; uncommitted offsets are
// redelivered and ON CONFLICT DO NOTHING makes that safe.
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart) {
    console.error('❌ Consumer crashed and will not auto-restart; exiting for the orchestrator to restart:', payload.error);
    process.exit(1);
  }
});

const startConsumer = async () => {
  try {
    console.log('🔄 Initializing Kafka Consumer...');
    await consumer.connect();
    await dlqProducer.connect();
    
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    console.log(`📥 Consumer subscribed to topic: ${KAFKA_TOPIC}`);

    await consumer.run({
      // We consume messages in batches to leverage database transaction speed
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        const timer = dbWriteDuration.startTimer();
        const pgClient = await db.connect();

        // Outcomes are only acted on (offsets resolved, DLQ sent, metrics bumped)
        // after the batch transaction commits. Resolving an offset or sending to
        // the DLQ before the COMMIT would let Kafka move past a message whose
        // insert is later rolled back — a silent, permanent data loss window.
        type Outcome =
          | { status: 'stored'; message: (typeof batch.messages)[number] }
          | { status: 'dlq'; message: (typeof batch.messages)[number] };
        const outcomes: Outcome[] = [];

        try {
          // Begin Database Transaction for the batch
          await pgClient.query('BEGIN');

          let i = 0;
          for (const message of batch.messages) {
            // Respect consumer cancellation tokens
            if (!isRunning() || isStale()) break;

            const savepoint = `sp_${i++}`;
            // A savepoint isolates one message's failure from the rest of the
            // batch: without it, a single bad insert aborts the whole Postgres
            // transaction and every subsequent message in the batch fails too.
            await pgClient.query(`SAVEPOINT ${savepoint}`);

            try {
              const rawValue = message.value?.toString();
              if (!rawValue) {
                throw new Error('Message value is null or empty');
              }

              const event = JSON.parse(rawValue);

              // SQL Batch Insertion with ON CONFLICT DO NOTHING (idempotency check)
              const inserted = await pgClient.query(
                `INSERT INTO events (id, device_id, event_type, value, timestamp)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO NOTHING
                 RETURNING id`,
                [
                  event.id,
                  event.deviceId,
                  event.eventType,
                  event.value,
                  event.timestamp,
                ]
              );

              // An existing row is only a harmless redelivery if it holds the
              // same payload. The gateway's payload fingerprint lives in Redis
              // and can be gone (state loss, lock expiry), so a replay of the
              // same key with different data can reach this point; keep the
              // first payload and dead-letter the conflict instead of dropping it.
              if (inserted.rowCount === 0) {
                const { rows } = await pgClient.query(
                  'SELECT device_id, event_type, value, timestamp FROM events WHERE id = $1',
                  [event.id]
                );
                const row = rows[0];
                if (
                  row &&
                  (String(row.device_id).toLowerCase() !== String(event.deviceId).toLowerCase() ||
                    row.event_type !== event.eventType ||
                    row.value !== event.value ||
                    Number(row.timestamp) !== event.timestamp)
                ) {
                  throw new Error(`Idempotency conflict: event ${event.id} already stored with a different payload`);
                }
              }

              await pgClient.query(`RELEASE SAVEPOINT ${savepoint}`);
              outcomes.push({ status: 'stored', message });
            } catch (err) {
              console.error('❌ Error processing single message, routing to DLQ:', err);
              await pgClient.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
              await pgClient.query(`RELEASE SAVEPOINT ${savepoint}`);
              outcomes.push({ status: 'dlq', message });
            }

            // Tell Kafka broker this consumer is still healthy
            await heartbeat();
          }

          // Commit database transaction
          await pgClient.query('COMMIT');
          timer();
        } catch (transactionError) {
          // Rollback the entire transaction on DB failures (e.g. database network error)
          await pgClient.query('ROLLBACK');
          console.error('❌ Transaction rolled back due to error:', transactionError);

          // Nothing in `outcomes` gets acted on: no offsets resolved and no DLQ
          // sends, so KafkaJS will redeliver this whole batch from the last
          // committed offset once retries reconnect.
          throw transactionError;
        } finally {
          pgClient.release();
        }

        // The DB transaction is durably committed at this point, so it's now
        // safe to fan out side effects and let Kafka advance past these offsets.
        for (const outcome of outcomes) {
          if (outcome.status === 'stored') {
            eventsProcessedCounter.inc();
          } else {
            eventsFailedCounter.inc();
            await dlqProducer.send({
              topic: DLQ_TOPIC,
              messages: [
                {
                  key: outcome.message.key,
                  value: outcome.message.value,
                },
              ],
            });
          }
          resolveOffset(outcome.message.offset);
        }
      },
    });
  } catch (error) {
    console.error('❌ Fatal error in Consumer loop:', error);
    process.exit(1);
  }
};

// Start metrics server for Prometheus scraping
const server = http.createServer(async (req, res) => {
  if (req.url === '/metrics') {
    res.setHeader('Content-Type', registry.contentType);
    res.end(await registry.metrics());
  } else if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'UP', service: 'metrics-consumer' }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(PORT, () => {
  console.log(`📊 Consumer Telemetry Server running on port ${PORT}`);
});

// Run consumer
startConsumer();

// Graceful Shutdown Handler
const shutdown = async (signal: string) => {
  console.log(`\n⚙️ Received ${signal}. Stopping consumer worker...`);
  
  server.close(async () => {
    console.log('🛑 Consumer Telemetry Server closed.');
    try {
      await consumer.disconnect();
      await dlqProducer.disconnect();
      console.log('🛑 Kafka connection closed.');
      process.exit(0);
    } catch (err) {
      console.error('❌ Error during consumer shutdown:', err);
      process.exit(1);
    }
  });
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
