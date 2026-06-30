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

// TODO: [Exercise - Kafka Partitioner Configuration]
// When starting the API Gateway, KafkaJS prints a startup warning:
// "KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior..."
// Task: Pass the configuration object inside `kafka.producer(...)` to specify the partitioner:
// 1. Resolve the warning by explicitly selecting the partitioner (e.g. Partitioners.LegacyPartitioner or a custom partitioner).
// 2. Test and verify that events for the same `deviceId` consistently route to the exact same partition topic.
// Tip: import { Partitioners } from 'kafkajs'; and configure it in the producer options:
// export const producer: Producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
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
