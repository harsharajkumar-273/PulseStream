import { Kafka, Producer } from 'kafkajs';
import { env } from './env.js';

const brokers = env.KAFKA_BROKERS.split(',');

export const kafka = new Kafka({
  clientId: 'pulsestream-gateway',
  brokers,
  retry: {
    initialRetryTime: 300,
    retries: 5,
  },
});

export const producer: Producer = kafka.producer();

export const connectKafka = async (): Promise<void> => {
  try {
    console.log('🔄 Connecting to Kafka/Redpanda broker...');
    await producer.connect();
    console.log('🚀 Connected to Kafka/Redpanda successfully');
  } catch (err) {
    console.error('❌ Failed to connect to Kafka/Redpanda:', err);
    process.exit(1);
  }
};

// Graceful shutdown helper
export const disconnectKafka = async (): Promise<void> => {
  try {
    console.log('⚙️ Disconnecting Kafka producer...');
    await producer.disconnect();
    console.log('🛑 Kafka producer disconnected.');
  } catch (err) {
    console.error('❌ Error during Kafka disconnection:', err);
  }
};
