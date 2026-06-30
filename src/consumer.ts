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

        try {
          // Begin Database Transaction for the batch
          await pgClient.query('BEGIN');

          for (const message of batch.messages) {
            // Respect consumer cancellation tokens
            if (!isRunning() || isStale()) break;

            try {
              const rawValue = message.value?.toString();
              if (!rawValue) {
                throw new Error('Message value is null or empty');
              }

              const event = JSON.parse(rawValue);

              // SQL Batch Insertion with ON CONFLICT DO NOTHING (idempotency check)
              await pgClient.query(
                `INSERT INTO events (id, device_id, event_type, value, timestamp)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (id) DO NOTHING`,
                [
                  event.id,
                  event.deviceId,
                  event.eventType,
                  event.value,
                  event.timestamp,
                ]
              );

              eventsProcessedCounter.inc();
              resolveOffset(message.offset);
            } catch (err) {
              console.error('❌ Error processing single message, routing to DLQ:', err);
              
              // Increment failed metrics counter
              eventsFailedCounter.inc();

              // Route poison pill to DLQ topic
              await dlqProducer.send({
                topic: DLQ_TOPIC,
                messages: [
                  {
                    key: message.key,
                    value: message.value,
                  },
                ],
              });

              // Commit offset anyway so we don't block subsequent events in the partition
              resolveOffset(message.offset);
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
          
          // Re-throw so KafkaJS handles reconnection retries
          throw transactionError; 
        } finally {
          pgClient.release();
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
