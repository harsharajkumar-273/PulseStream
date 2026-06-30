import { Router, Request, Response, NextFunction } from 'express';
import { validate } from '../middleware/validate.js';
import { CreateEventSchema } from '../schemas/event.js';
import { authenticate } from '../middleware/auth.js';
import { enforceIdempotency } from '../middleware/idempotency.js';
import { rateLimiter } from '../middleware/rateLimiter.js';
import redis from '../config/redis.js';
import { producer } from '../config/kafka.js';

const router = Router();
const KAFKA_TOPIC = 'metrics.raw';

// Ingestion endpoint: POST /v1/events
router.post(
  '/events',
  authenticate,
  rateLimiter,
  enforceIdempotency,
  validate(CreateEventSchema),
  async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const eventData = req.body;
      const idempotencyKey = req.header('Idempotency-Key')!;

      const payload = {
        id: idempotencyKey,
        deviceId: eventData.deviceId,
        eventType: eventData.eventType,
        value: eventData.value,
        timestamp: eventData.timestamp,
        metadata: eventData.metadata,
      };

      // 1. Publish the event to Kafka/Redpanda topic 'metrics.raw'
      // We key the message by deviceId to ensure ordering guarantees per device
      await producer.send({
        topic: KAFKA_TOPIC,
        messages: [
          {
            key: eventData.deviceId,
            value: JSON.stringify(payload),
          },
        ],
      });

      // 2. Publish to Redis Pub/Sub for real-time WebSocket dashboard streaming
      await redis.publish('events:stream', JSON.stringify(payload));

      // Return 202 Accepted (Processing is asynchronous)
      res.status(202).json({
        status: 'success',
        message: 'Event accepted for processing',
        data: {
          id: idempotencyKey,
          deviceId: eventData.deviceId,
          timestamp: eventData.timestamp,
        },
      });
    } catch (error) {
      next(error);
    }
  }
);

export default router;
