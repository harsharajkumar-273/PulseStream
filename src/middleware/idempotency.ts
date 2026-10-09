import { createHash } from 'crypto';
import { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import redis from '../config/redis.js';

const uuidSchema = z.string().uuid('Idempotency-Key must be a valid UUID v4');

// Key-order-independent JSON, so {a,b} and {b,a} fingerprint identically.
const canonicalize = (v: unknown): unknown =>
  Array.isArray(v)
    ? v.map(canonicalize)
    : v && typeof v === 'object'
      ? Object.fromEntries(
          Object.entries(v as Record<string, unknown>)
            .sort(([a], [b]) => (a < b ? -1 : 1))
            .map(([k, val]) => [k, canonicalize(val)])
        )
      : v;

const fingerprint = (body: unknown): string =>
  createHash('sha256').update(JSON.stringify(canonicalize(body ?? null))).digest('hex');

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
  // Stored with the key so the same key can't silently carry a different payload.
  const fp = fingerprint(req.body);

  try {
    // Attempt to acquire an execution lock with 10 seconds TTL
    // NX: Only set if the key does not exist
    const lockAcquired = await redis.set(redisKey, `IN_PROGRESS:${fp}`, 'EX', 10, 'NX');

    if (!lockAcquired) {
      // Key exists! Fetch the status
      const currentValue = await redis.get(redisKey);

      // Formats: IN_PROGRESS:<fingerprint> | RESOLVED:<fingerprint>:<json>
      const [state, storedFp] = (currentValue ?? '').split(':', 2);

      if ((state === 'IN_PROGRESS' || state === 'RESOLVED') && storedFp !== fp) {
        res.status(422).json({
          status: 'error',
          message: 'Idempotency-Key was already used with a different request payload',
        });
        return;
      }

      if (state === 'IN_PROGRESS') {
        // Active request in flight: return 409 Conflict
        res.status(409).json({
          status: 'error',
          message: 'A request with this Idempotency-Key is already in progress',
        });
        return;
      }

      if (state === 'RESOLVED' && currentValue) {
        // Request was already processed: serve cached response
        const cachedResponseStr = currentValue.substring('RESOLVED:'.length + fp.length + 1);
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
          .set(redisKey, `RESOLVED:${fp}:${JSON.stringify(responseData)}`, 'EX', 86400)
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
