import { Server as HTTPServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { subRedis } from './redis.js';

// Extend the WebSocket interface to track client health (liveness)
interface ExtWebSocket extends WebSocket {
  isAlive: boolean;
}

const REDIS_CHANNEL = 'events:stream';

export const initWebSocketServer = (httpServer: HTTPServer): void => {
  const wss = new WebSocketServer({ noServer: true });

  // Handle the HTTP upgrade handshake manually to allow path filtering
  httpServer.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url || '', `http://${request.headers.host}`);
    
    if (url.pathname === '/v1/stream') {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } else {
      // Reject any upgrade request not targeting our stream endpoint
      socket.destroy();
    }
  });

  console.log('📡 WebSocket Server attached to Ingestion HTTP Server');

  wss.on('connection', (ws: ExtWebSocket) => {
    ws.isAlive = true;

    console.log(`🔌 New WS Client connected. Active clients: ${wss.clients.size}`);

    // Listen for the client's pong response
    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('close', () => {
      console.log(`❌ WS Client disconnected. Active clients: ${wss.clients.size}`);
    });

    ws.on('error', (error) => {
      console.error('💥 WebSocket Socket Error:', error);
    });
  });

  // 1. Setup Ping/Pong Heartbeat to clean up dead/half-open sockets
  const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
      const extWs = ws as ExtWebSocket;
      if (extWs.isAlive === false) {
        console.log('🗑️ Terminating inactive WS client');
        return extWs.terminate();
      }

      extWs.isAlive = false;
      extWs.ping(); // Send ping frame (client's browser will auto-respond with pong)
    });
  }, 30000); // Check every 30 seconds

  // 2. Subscribe to Redis event stream channel
  subRedis.subscribe(REDIS_CHANNEL, (err: unknown) => {
    if (err) {
      console.error(`❌ Failed to subscribe to Redis channel ${REDIS_CHANNEL}:`, err);
      return;
    }
    console.log(`📥 Subscribed to Redis Pub/Sub channel: ${REDIS_CHANNEL}`);
  });

  // 3. Broadcast incoming Redis stream updates to all connected WS clients
  subRedis.on('message', (channel: string, message: string) => {
    if (channel === REDIS_CHANNEL) {
      wss.clients.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(message);
        }
      });
    }
  });

  // Cleanup on server shutdown
  wss.on('close', () => {
    clearInterval(heartbeatInterval);
    console.log('🛑 WebSocket Server stopped.');
  });
};
