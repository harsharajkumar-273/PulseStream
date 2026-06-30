import pg from 'pg';
import { env } from './env.js';

const { Pool } = pg;

// Create a database connection pool
const db = new Pool({
  connectionString: env.DATABASE_URL,
  max: 20, // Max number of clients in the pool
  idleTimeoutMillis: 30000, // Close idle connections after 30s
  connectionTimeoutMillis: 2000, // Fail fast if connection takes > 2s
});

db.on('connect', () => {
  console.log('🐘 PostgreSQL connection pool initialized');
});

db.on('error', (err) => {
  console.error('❌ Unexpected error on idle PostgreSQL client:', err);
});

export default db;
