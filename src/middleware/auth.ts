import { Request, Response, NextFunction } from 'express';
import db from '../config/db.js';
import redis from '../config/redis.js';

// Extend Express Request interface to hold authenticated client metadata
declare global {
  namespace Express {
    interface Request {
      client?: {
        id: string;
        name: string;
      };
    }
  }
}

export const authenticate = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const apiKey = req.header('x-api-key');

    if (!apiKey) {
      res.status(401).json({
        status: 'error',
        message: 'Unauthorized: Missing API Key in x-api-key header',
      });
      return;
    }

    const cacheKey = `client:key:${apiKey}`;

    // TODO: [Exercise - Redis Outage Circuit Breaker]
    // Currently, if Redis goes down, `redis.get` will throw an error, causing this
    // middleware to throw an HTTP 500 error and shut down all ingestion.
    // Task: Wrap the Redis cache get/set logic in a try-catch block. If Redis fails,
    // catch the error, log a warning (e.g. '⚠️ Redis offline, falling back to DB'), 
    // and query the PostgreSQL database directly so the gateway continues running.
    // Tip: Use a short-lived local memory cache (like a simple Map or LRU cache)
    // to store valid keys locally for 30 seconds during a Redis outage, protecting
    // your PostgreSQL pool from getting flooded.

    // 1. Try to fetch client from Redis Cache
    const cachedClient = await redis.get(cacheKey);

    if (cachedClient) {
      req.client = JSON.parse(cachedClient);
      next();
      return;
    }

    // 2. Cache Miss: Query PostgreSQL
    const query = 'SELECT id, name, active FROM clients WHERE api_key = $1 LIMIT 1';
    const result = await db.query(query, [apiKey]);

    if (result.rows.length === 0) {
      res.status(401).json({
        status: 'error',
        message: 'Unauthorized: Invalid API Key',
      });
      return;
    }

    const client = result.rows[0];

    if (!client.active) {
      res.status(401).json({
        status: 'error',
        message: 'Unauthorized: API Key has been deactivated',
      });
      return;
    }

    const clientMetadata = { id: client.id, name: client.name };

    // 3. Cache the valid API Key in Redis for 5 minutes (300 seconds)
    await redis.setex(cacheKey, 300, JSON.stringify(clientMetadata));

    req.client = clientMetadata;
    next();
  } catch (error) {
    next(error);
  }
};
