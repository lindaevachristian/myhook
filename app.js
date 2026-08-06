// app.js - Cloudflare Workers Version
// WebSocket to TCP Proxy with KV YUMI

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1);
    
    // Welcome page
    if (!path || path === '') {
      try {
        const stats = await env.YUMI.get('stats', 'json') || { 
          total: 0, 
          active: 0 
        };
        
        return new Response(
          `WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n` +
          `Total Connections: ${stats.total || 0}\n` +
          `Active Connections: ${stats.active || 0}\n`,
          { 
            headers: { 'Content-Type': 'text/plain' }
          }
        );
      } catch (e) {
        return new Response(
          `WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!\n`,
          { headers: { 'Content-Type': 'text/plain' } }
        );
      }
    }

    // Handle WebSocket upgrade
    if (request.headers.get('Upgrade') === 'websocket') {
      try {
        // Decode base64 target
        const decoded = atob(path);
        const [host, port] = decoded.split(':');
        
        if (!host || !port) {
          return new Response('Invalid target format. Use: base64(host:port)', { 
            status: 400 
          });
        }

        // Create WebSocket pair
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        server.accept();

        // Connect to TCP target
        const tcp = await connectTCP(host, parseInt(port));
        
        // Update stats
        await updateStats(env, 'add');
        
        // Start proxy
        proxyConnection(server, tcp, env, host, port);
        
        return new Response(null, {
          status: 101,
          webSocket: client
        });
        
      } catch (err) {
        return new Response(`Error: ${err.message}`, { 
          status: 400 
        });
      }
    }

    return new Response('WebSocket endpoint only. Use: wss://worker.dev/base64(host:port)', { 
      status: 400 
    });
  }
};

// TCP Connection Helper
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
        reject(new Error(err.message || 'Connection failed'));
      };

      socket.connect(host, port);
    } catch (err) {
      reject(err);
    }
  });
}

// Proxy Handler
function proxyConnection(ws, tcp, env, host, port) {
  let closed = false;
  let pingInterval = null;

  // Close all connections
  const closeAll = async () => {
    if (closed) return;
    closed = true;
    
    // Clear ping interval
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }
    
    // Update stats
    await updateStats(env, 'remove');
    
    try { if (ws.readyState === 1) ws.close(); } catch (e) {}
    try { if (tcp.readyState === 1) tcp.close(); } catch (e) {}
  };

  // WebSocket → TCP
  ws.addEventListener('message', async (e) => {
    if (closed || tcp.readyState !== 1) return;
    
    try {
      let data = e.data;
      
      if (typeof data === 'string') {
        // Ensure newline for stratum protocol
        if (!data.endsWith('\n')) {
          data += '\n';
        }
        await tcp.write(data);
      } else if (data instanceof ArrayBuffer) {
        await tcp.write(new Uint8Array(data));
      } else if (data instanceof Uint8Array) {
        await tcp.write(data);
      }
    } catch (err) {
      closeAll();
    }
  });

  // TCP → WebSocket
  const forwardTCP = async () => {
    try {
      const reader = tcp.readable.getReader();
      
      while (true) {
        const { value, done } = await reader.read();
        
        if (done || closed) {
          break;
        }

        if (ws.readyState === 1) {
          try {
            if (value instanceof ArrayBuffer) {
              ws.send(value);
            } else if (value instanceof Uint8Array) {
              ws.send(value.buffer);
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

  // Start forwarding
  forwardTCP();

  // Event handlers
  ws.addEventListener('close', () => closeAll());
  ws.addEventListener('error', () => closeAll());
  
  if (tcp.onclose) tcp.onclose = () => closeAll();
  if (tcp.onerror) tcp.onerror = () => closeAll();

  // Keep-alive ping
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

// KV Stats Update
async function updateStats(env, action) {
  try {
    const key = 'stats';
    let stats = await env.YUMI.get(key, 'json') || { total: 0, active: 0 };
    
    if (action === 'add') {
      stats.total += 1;
      stats.active += 1;
    } else if (action === 'remove') {
      stats.active -= 1;
      if (stats.active < 0) stats.active = 0;
    }
    
    await env.YUMI.put(key, JSON.stringify(stats));
  } catch (e) {
    // Ignore KV errors
  }
}
