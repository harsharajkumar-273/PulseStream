import { Request, Response, NextFunction } from 'express';
import { env } from '../config/env.js';

export const errorHandler = (
  err: any,
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  const statusCode = err.status || err.statusCode || 500;
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
