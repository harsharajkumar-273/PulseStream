import { Request, Response, NextFunction } from 'express';
import { env } from '../config/env.js';

export const errorHandler = (
  err: any,
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  // ioredis command timeouts mean Redis is unreachable: report unavailability, not a bug.
  const redisDown = typeof err.message === 'string' && err.message.includes('Command timed out');
  const statusCode = redisDown ? 503 : err.status || err.statusCode || 500;
  const isProd = env.NODE_ENV === 'production';

  // Log the error (we can update this later to structured JSON logging)
  console.error('💥 Unhandled Error:', {
    message: err.message,
    stack: isProd ? undefined : err.stack,
    path: req.path,
    method: req.method,
  });

  res.status(statusCode).json({
    status: 'error',
    message: err.message || 'Internal Server Error',
    ...(isProd ? {} : { stack: err.stack }),
  });
};
