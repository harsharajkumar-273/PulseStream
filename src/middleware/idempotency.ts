import { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import redis from '../config/redis.js';

const uuidSchema = z.string().uuid('Idempotency-Key must be a valid UUID v4');

export const enforceIdempotency = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const idempotencyKey = req.header('Idempotency-Key');

  if (!idempotencyKey) {
    res.status(400).json({
      status: 'error',
      message: 'Missing Idempotency-Key header',
    });
    return;
  }

  // Validate UUID format
  const parsed = uuidSchema.safeParse(idempotencyKey);
  if (!parsed.success) {
    res.status(400).json({
      status: 'error',
      message: parsed.error.issues[0].message,
    });
    return;
  }

  const redisKey = `idempotency:key:${idempotencyKey}`;

  try {
    // Attempt to acquire an execution lock with 10 seconds TTL
    // NX: Only set if the key does not exist
    const lockAcquired = await redis.set(redisKey, 'IN_PROGRESS', 'EX', 10, 'NX');

    if (!lockAcquired) {
      // Key exists! Fetch the status
      const currentValue = await redis.get(redisKey);

      if (currentValue === 'IN_PROGRESS') {
        // Active request in flight: return 409 Conflict
        res.status(409).json({
          status: 'error',
          message: 'A request with this Idempotency-Key is already in progress',
        });
        return;
      }

      if (currentValue && currentValue.startsWith('RESOLVED:')) {
        // Request was already processed: serve cached response
        const cachedResponseStr = currentValue.substring('RESOLVED:'.length);
        const cachedResponse = JSON.parse(cachedResponseStr);

        res.status(cachedResponse.statusCode).json(cachedResponse.body);
        return;
      }

      // Fallback
      res.status(500).json({
        status: 'error',
        message: 'Internal server state error regarding idempotency',
      });
      return;
    }

    // Intercept res.json to capture response and save to Redis on success
    const originalJson = res.json;
    res.json = function (body): Response {
      // Capture only successful/acceptable status codes for idempotency storage (e.g. 2xx, 4xx)
      // Standard practice: cache successful processing, but avoid caching transient server errors (5xx)
      if (res.statusCode >= 200 && res.statusCode < 500) {
        const responseData = {
          statusCode: res.statusCode,
          body,
        };
        // Store resolved response in Redis with a 24-hour (86400 seconds) expiration
        redis
          .set(redisKey, `RESOLVED:${JSON.stringify(responseData)}`, 'EX', 86400)
          .catch((err: any) => {
            console.error('❌ Failed to save response to idempotency cache:', err);
          });
      } else {
        // If it's a 5xx error, delete the lock so client can retry immediately
        redis.del(redisKey).catch((err: any) => {
          console.error('❌ Failed to release idempotency lock after 5xx error:', err);
        });
      }

      return originalJson.call(this, body);
    };

    next();
  } catch (error) {
    next(error);
  }
};
