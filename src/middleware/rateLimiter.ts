import { Request, Response, NextFunction } from 'express';
import redis from '../config/redis.js';

// Configuration: Allow 100 requests per 60 seconds per client
const WINDOW_SIZE_IN_SECONDS = 60;
const MAX_REQUESTS_ALLOWED = 100;

export const rateLimiter = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  // Identify client by authenticated client ID, falling back to IP address
  const clientId = req.client?.id || req.ip || 'anonymous';
  const redisKey = `rate_limit:${clientId}`;

  try {
    // TODO: [Exercise - Redis Sliding Window Rate Limiter]
    // Implement a sliding-window rate limiter using Redis sorted sets (zset).
    //
    // Algorithm Steps:
    // 1. Get the current timestamp in milliseconds (e.g., const now = Date.now()).
    // 2. Define the window boundary (e.g., const clearBefore = now - WINDOW_SIZE_IN_SECONDS * 1000).
    // 3. Execute a Redis transaction (pipeline) to perform these operations atomically:
    //    a. ZREMRANGEBYSCORE: Remove elements (timestamps) older than 'clearBefore'.
    //    b. ZADD: Add the current timestamp 'now' with a unique member value (e.g. timestamp or random string) to the sorted set.
    //    c. ZCARD: Get the count of active members in the sorted set.
    //    d. EXPIRE: Update the key's TTL to at least WINDOW_SIZE_IN_SECONDS to clean up idle keys.
    // 4. Retrieve the count from the transaction results.
    // 5. If the count exceeds MAX_REQUESTS_ALLOWED:
    //    - Set HTTP headers: 'Retry-After' (in seconds) and 'X-RateLimit-Limit' / 'X-RateLimit-Remaining'.
    //    - Return an HTTP 429 Too Many Requests response with a JSON error body.
    // 6. If the count is within limits, call next() to allow the request.
    //
    // Resources:
    // - ioredis transactions: redis.multi().zremrangebyscore(...).zadd(...).zcard(...).pexpire(...).exec()

    // Bypassing rate limiting for now (skeleton fallback)
    next();
  } catch (error) {
    console.error('❌ Rate Limiter Error:', error);
    // In production, we fail-open (call next()) to prevent a Redis crash from taking down the API,
    // or fail-closed based on risk posture. Let's fail-open for now.
    next();
  }
};
