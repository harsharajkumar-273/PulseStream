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

  // TODO: [Exercise - Security Information Disclosure]
  // Currently, `err.message` is returned directly to the client API response.
  // Vulnerability: If a database query fails or a network resource timeouts in production,
  // `err.message` will expose raw SQL queries, constraint names, or hostnames to the client,
  // leaking internal system design structures.
  // Task: Add sanitization. If the environment is production (isProd is true) AND the
  // error is a 500 Server Error (or unexpected error), replace `err.message` with a generic
  // 'Internal Server Error' message. Keep the original detailed error logged inside
  // console.error so it is still captured securely in system logs!

  res.status(statusCode).json({
    status: 'error',
    message: err.message || 'Internal Server Error',
    ...(isProd ? {} : { stack: err.stack }),
  });
};
