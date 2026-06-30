import app from './app.js';
import { env } from './config/env.js';
import { initWebSocketServer } from './config/websocket.js';
import { connectKafka, disconnectKafka } from './config/kafka.js';

// Connect to Kafka/Redpanda before starting the server
await connectKafka();

const server = app.listen(env.PORT, () => {
  console.log(`🚀 PulseStream API Ingestion Gateway running on port ${env.PORT} in ${env.NODE_ENV} mode`);
});

// Initialize WebSocket server connection handler
initWebSocketServer(server);

// Handle graceful shutdown signals
const shutdown = async (signal: string) => {
  console.log(`\n⚙️ Received ${signal}. Shutting down gateway gracefully...`);
  
  // Close the HTTP & WebSocket server
  server.close(async () => {
    console.log('🛑 Ingestion Gateway closed.');
    // Disconnect Kafka producer
    await disconnectKafka();
    process.exit(0);
  });
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
