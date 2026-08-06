// app.js - Cloudflare Workers WebSocket Proxy
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1);
    
    // Welcome page
    if (!path || path === '') {
      return new Response('WELCOME TO MCP-CLIENT-NODE PUBLIC! FEEL FREE TO USE!', {
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // Handle WebSocket
    if (request.headers.get('Upgrade') === 'websocket') {
      try {
        // Decode base64 target
        const decoded = atob(path);
        const [host, port] = decoded.split(':');
        
        if (!host || !port) {
          return new Response('Invalid target', { status: 400 });
        }

        // Create WebSocket pair
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        server.accept();

        // Connect to TCP
        const tcp = await connectTCP(host, parseInt(port));
        
        // Start proxy
        handleProxy(server, tcp);
        
        return new Response(null, {
          status: 101,
          webSocket: client
        });
      } catch (err) {
        return new Response(`Error: ${err.message}`, { status: 400 });
      }
    }

    return new Response('Use WebSocket protocol', { status: 400 });
  }
};

// TCP Connection
async function connectTCP(host, port) {
  return new Promise((resolve, reject) => {
    const socket = new Socket();
    const timeout = setTimeout(() => {
      socket.close();
      reject(new Error('Timeout'));
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
  });
}

// Proxy Handler
function handleProxy(ws, tcp) {
  let closed = false;

  const closeAll = () => {
    if (closed) return;
    closed = true;
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

  ws.addEventListener('close', closeAll);
  ws.addEventListener('error', closeAll);
  
  if (tcp.onclose) tcp.onclose = closeAll;
  if (tcp.onerror) tcp.onerror = closeAll;

  // Keep alive
  const pingInterval = setInterval(() => {
    if (!closed && ws.readyState === 1) {
      try { ws.ping(); } catch (e) { closeAll(); }
    }
  }, 30000);

  // Override close to clear interval
  const originalClose = closeAll;
  closeAll = () => {
    clearInterval(pingInterval);
    originalClose();
  };
}
