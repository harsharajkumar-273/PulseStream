import { z } from 'zod';

export const CreateEventSchema = z.object({
  deviceId: z.string().uuid('deviceId must be a valid UUID v4'),
  eventType: z.string().min(2).max(50),
  value: z.number(),
  timestamp: z.number().int().positive().refine((val) => {
    const now = Date.now();
    const fiveMinutes = 5 * 60 * 1000;
    return val >= now - fiveMinutes && val <= now + fiveMinutes;
  }, 'Timestamp must be within 5 minutes of server time (clock drift check)'),
  metadata: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
});

export type CreateEventInput = z.infer<typeof CreateEventSchema>;
