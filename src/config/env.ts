import { z } from 'zod';
import dotenv from 'dotenv';

// Load .env file
dotenv.config();

const envSchema = z.object({
  PORT: z.string().default('3000').transform((val) => parseInt(val, 10)),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  DATABASE_URL: z.string().default('postgresql://postgres:postgres@localhost:5432/pulsestream'),
  KAFKA_BROKERS: z.string().default('localhost:9092'),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌ Invalid environment variables:', parsed.error.format());
  process.exit(1);
}

export const env = parsed.data;
