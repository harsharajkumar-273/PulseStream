import { Redis } from 'ioredis';
import { env } from './env.js';

// Create a redis client instance
const redis = new Redis(env.REDIS_URL, {
  maxRetriesPerRequest: null, // Critical for robust connection retries
  retryStrategy(times: number) {
    const delay = Math.min(times * 100, 3000);
    return delay;
  },
});

redis.on('connect', () => {
  console.log('🔌 Connected to Redis (Command Client) successfully');
});

redis.on('error', (err: unknown) => {
  console.error('❌ Redis Command Client Error:', err);
});

// Create a duplicate connection specifically for blocking subscriptions
export const subRedis = redis.duplicate();

subRedis.on('connect', () => {
  console.log('🔌 Connected to Redis (Subscription Client) successfully');
});

subRedis.on('error', (err: unknown) => {
  console.error('❌ Redis Subscription Client Error:', err);
});

export default redis;
