import { Request, Response, NextFunction } from 'express';
import client from 'prom-client';

// Dedicated registry for Gateway metrics
export const metricsRegistry = new client.Registry();

// Collect default system metrics (CPU, memory, garbage collection stats)
client.collectDefaultMetrics({ register: metricsRegistry });

const httpRequestDurationMicroseconds = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5],
  registers: [metricsRegistry],
});

const httpRequestsCounter = new client.Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code'],
  registers: [metricsRegistry],
});

const activeRequestsGauge = new client.Gauge({
  name: 'http_active_requests',
  help: 'Number of active HTTP requests currently processing',
  registers: [metricsRegistry],
});

export const metricsMiddleware = (req: Request, res: Response, next: NextFunction): void => {
  // Exclude health check and metrics scraping from metrics collection to avoid telemetry noise
  if (req.path === '/metrics' || req.path === '/health') {
    next();
    return;
  }

  activeRequestsGauge.inc();
  const startTime = process.hrtime();

  // Listen to the response finish event to calculate duration
  res.on('finish', () => {
    activeRequestsGauge.dec();
    const diff = process.hrtime(startTime);
    const durationInSeconds = diff[0] + diff[1] / 1e9;

    // Resolve matching route pattern (e.g. /v1/events instead of dynamic paths)
    const route = req.route ? req.route.path : req.path;
    const statusCode = res.statusCode.toString();

    httpRequestsCounter.labels(req.method, route, statusCode).inc();
    httpRequestDurationMicroseconds.labels(req.method, route, statusCode).observe(durationInSeconds);
  });

  next();
};
