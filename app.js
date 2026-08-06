// app.js - Fixed Version
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1);
    
    // Welcome page
    if (!path || path === '') {
      let stats = await env.YUMI.get('stats', 'json') || { 
        total_connections: 0,
        active_connections: 0
      };
      
      return new Response(
        `WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n\n` +
        `📊 Statistics:\n` +
        `Total Connections: ${stats.total_connections}\n` +
        `Active Connections: ${stats.active_connections}\n` +
        `KV Namespace: YUMI (Connected)\n`,
        { 
          headers: { 
            'Content-Type': 'text/plain'
          }
        }
      );
    }

    // Handle WebSocket
    if (request.headers.get('Upgrade') === 'websocket') {
      try {
        const decoded = atob(path);
        const [host, port] = decoded.split(':');
        
        if (!host || !port) {
          return new Response('Invalid target', { status: 400 });
        }

        // Update stats di KV
        let stats = await env.YUMI.get('stats', 'json') || {
          total_connections: 0,
          active_connections: 0,
          connections: {}
        };
        
        const connId = crypto.randomUUID();
        stats.total_connections += 1;
        stats.active_connections += 1;
        stats.connections[connId] = {
          host: host,
          port: port,
          connected_at: new Date().toISOString()
        };
        
        await env.YUMI.put('stats', JSON.stringify(stats));

        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        server.accept();

        // Connect to TCP
        try {
          const tcpSocket = await connectTCP(host, parseInt(port));
          handleProxy(server, tcpSocket, host, port, connId, env);
          
          return new Response(null, {
            status: 101,
            webSocket: client
          });
        } catch (err) {
          // Update stats on error
          stats.active_connections -= 1;
          delete stats.connections[connId];
          await env.YUMI.put('stats', JSON.stringify(stats));
          
          server.close(1011, err.message);
          return new Response(`Failed: ${err.message}`, { status: 502 });
        }
      } catch (err) {
        return new Response(`Error: ${err.message}`, { status: 400 });
      }
    }

    return new Response('WebSocket endpoint only', { status: 400 });
  }
};

async function connectTCP(host, port) {
  return new Promise((resolve, reject) => {
    try {
      const socket = new Socket();
      const timeout = setTimeout(() => {
        socket.close();
        reject(new Error('Connection timeout'));
      }, 10000);

      socket.onopen = () => {
        clearTimeout(timeout);
        resolve(socket);
      };

      socket.onerror = (err) => {
        clearTimeout(timeout);
        reject(new Error(`Socket error: ${err.message}`));
      };

      socket.connect(host, port);
    } catch (err) {
      reject(new Error(`Failed: ${err.message}`));
    }
  });
}

function handleProxy(ws, tcp, host, port, connId, env) {
  let closed = false;
  let pingInterval = null;

  // Function to close all connections
  const closeAll = async () => {
    if (closed) return;
    closed = true;
    
    // Clear ping interval
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
    
    // Update KV stats
    try {
      let stats = await env.YUMI.get('stats', 'json') || {
        total_connections: 0,
        active_connections: 0,
        connections: {}
      };
      
      stats.active_connections -= 1;
      if (stats.active_connections < 0) stats.active_connections = 0;
      delete stats.connections[connId];
      
      await env.YUMI.put('stats', JSON.stringify(stats));
    } catch (e) {
      // Ignore KV errors
    }
    
    try { if (ws.readyState === 1) ws.close(); } catch (e) {}
    try { if (tcp.readyState === 1) tcp.close(); } catch (e) {}
  };

  // WS → TCP
  ws.addEventListener('message', async (event) => {
    if (closed || tcp.readyState !== 1) return;
    try {
      let data = event.data;
      if (typeof data === 'string') {
        if (!data.endsWith('\n')) data += '\n';
        await tcp.write(data);
      } else if (data instanceof ArrayBuffer) {
        await tcp.write(new Uint8Array(data));
      }
    } catch (err) {
      closeAll();
    }
  });

  // TCP → WS
  const forwardTCP = async () => {
    try {
      const reader = tcp.readable.getReader();
      while (true) {
        const { value, done } = await reader.read();
        if (done || closed) break;
        if (ws.readyState === 1) {
          try {
            if (value instanceof ArrayBuffer) {
              ws.send(value);
            } else {
              ws.send(new TextDecoder().decode(value));
            }
          } catch (err) {
            break;
          }
        }
      }
    } catch (err) {
      // Ignore
    } finally {
      closeAll();
    }
  };

  forwardTCP();

  ws.addEventListener('close', () => closeAll());
  ws.addEventListener('error', () => closeAll());
  
  if (tcp.onclose) tcp.onclose = () => closeAll();
  if (tcp.onerror) tcp.onerror = () => closeAll();

  // Keep alive
  pingInterval = setInterval(() => {
    if (!closed && ws.readyState === 1) {
      try {
        ws.ping();
      } catch (e) {
        closeAll();
      }
    }
  }, 25000);
}
