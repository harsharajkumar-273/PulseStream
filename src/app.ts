import express from 'express';
import cors from 'cors';
import eventRouter from './routes/event.js';
import { errorHandler } from './middleware/errorHandler.js';
import { metricsMiddleware, metricsRegistry } from './middleware/metrics.js';

const app = express();

// Enable Cross-Origin Resource Sharing
app.use(cors());

// Register Prometheus metrics collector middleware
app.use(metricsMiddleware);

// Defense 1: Strict payload size limit of 10KB to prevent event loop starvation
app.use(express.json({ limit: '10kb' }));

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP', timestamp: new Date().toISOString() });
});

// Prometheus scraping endpoint
app.get('/metrics', async (req, res) => {
  try {
    res.setHeader('Content-Type', metricsRegistry.contentType);
    res.send(await metricsRegistry.metrics());
  } catch (error) {
    res.status(500).end(error);
  }
});

// Mount routes under /v1
app.use('/v1', eventRouter);

// Fallback for unhandled routes (404)
app.use((req, res, next) => {
  res.status(404).json({
    status: 'error',
    message: `Cannot ${req.method} ${req.path}`,
  });
});

// Register global error handler (MUST be the last middleware)
app.use(errorHandler);

export default app;
