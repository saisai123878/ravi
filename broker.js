/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║              WebSocket Broker Server  –  broker.js           ║
 * ║  A middleman that routes messages between connected clients  ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * ── DEPENDENCIES ───────────────────────────────────────────────
 *   npm install ws
 *
 * ── RUN ────────────────────────────────────────────────────────
 *   node broker.js
 *   PORT=9000 node broker.js          ← custom port
 *
 * ── MESSAGE PROTOCOL ───────────────────────────────────────────
 *
 *   1) Register immediately after connecting:
 *      { "type": "register", "name": "alice" }
 *
 *   2) Send a message to another user:
 *      { "type": "message", "to": "bob", "message": "hello!" }
 *
 *   Server → Client responses:
 *
 *   Registration success (JSON):
 *      { "type": "system", "message": "Registered as alice" }
 *
 *   Incoming message (plain text – ready to display as-is):
 *      From alice: hello!
 *
 *   User offline (JSON error):
 *      { "type": "error", "message": "User 'bob' is not online." }
 *
 *   Bad payload (JSON error):
 *      { "type": "error", "message": "Invalid JSON payload." }
 *
 * ── EXAMPLE TEST CLIENT ────────────────────────────────────────
 *   Save the block below as  client.js  and run two terminals:
 *
 *     Terminal 1:  node client.js alice bob
 *     Terminal 2:  node client.js bob   alice
 *
 * ──────────────────────────────────────────────────────────────
 *   // client.js
 *   const WebSocket = require("ws");
 *
 *   const NAME = process.argv[2] || "user1";
 *   const TO   = process.argv[3] || "user2";
 *   const ws   = new WebSocket("ws://localhost:8080");
 *
 *   ws.on("open", () => {
 *     console.log(`[${NAME}] Connected`);
 *     ws.send(JSON.stringify({ type: "register", name: NAME }));
 *
 *     setTimeout(() => {
 *       ws.send(JSON.stringify({
 *         type: "message",
 *         to: TO,
 *         message: `Hi ${TO}, this is ${NAME}!`
 *       }));
 *     }, 1500);
 *   });
 *
 *   ws.on("message", (raw) => console.log(`[${NAME}] <-`, raw.toString()));
 *   ws.on("close",   ()    => console.log(`[${NAME}] Disconnected`));
 *   ws.on("error",   (e)   => console.error(`[${NAME}] Error:`, e.message));
 *
 * ── PACKAGE.JSON ───────────────────────────────────────────────
 *   {
 *     "name": "ws-broker",
 *     "version": "1.0.0",
 *     "description": "WebSocket message broker",
 *     "main": "broker.js",
 *     "scripts": { "start": "node broker.js" },
 *     "dependencies": { "ws": "^8.18.0" }
 *   }
 * ──────────────────────────────────────────────────────────────
 */

"use strict";

const WebSocket = require("ws");

// ─── Configuration ─────────────────────────────────────────────────────────────

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;

// ─── Active client registry ────────────────────────────────────────────────────
//   Map<username: string  ->  socket: WebSocket>

const clients = new Map();

// ─── Utility: send a JSON control frame ────────────────────────────────────────

function sendJSON(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

// ─── Utility: send a plain-text data frame (final delivery to recipient) ───────

function sendPlainText(ws, text) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(text); // raw string – no JSON wrapper
  }
}

// ─── Handler: "register" ───────────────────────────────────────────────────────

function handleRegister(ws, payload) {
  const name = (payload.name || "").trim();

  if (!name) {
    sendJSON(ws, {
      type: "error",
      message: "Registration failed: 'name' is required.",
    });
    return;
  }

  if (!/^[a-zA-Z0-9_-]{1,32}$/.test(name)) {
    sendJSON(ws, {
      type: "error",
      message:
        "Registration failed: name must be 1-32 alphanumeric characters (_, - allowed).",
    });
    return;
  }

  if (clients.has(name)) {
    sendJSON(ws, {
      type: "error",
      message: `Registration failed: '${name}' is already taken.`,
    });
    return;
  }

  // Evict any previous registration on this socket (re-register scenario)
  if (ws._brokerName) {
    clients.delete(ws._brokerName);
    console.log(`[BROKER] Re-register : '${ws._brokerName}' -> '${name}'`);
  }

  ws._brokerName = name; // tag socket for disconnect cleanup
  clients.set(name, ws);

  console.log(
    `[BROKER] Registered  : '${name}'  (active users: ${clients.size})`
  );
  sendJSON(ws, { type: "system", message: `Registered as ${name}` });
}

// ─── Handler: "message" ────────────────────────────────────────────────────────

function handleMessage(ws, payload) {
  const sender = ws._brokerName;

  if (!sender) {
    sendJSON(ws, {
      type: "error",
      message: "You must register before sending messages.",
    });
    return;
  }

  const to   = (payload.to      || "").trim();
  const body = (payload.message || "").trim();

  if (!to) {
    sendJSON(ws, { type: "error", message: "Field 'to' is required." });
    return;
  }

  if (!body) {
    sendJSON(ws, {
      type: "error",
      message: "Field 'message' cannot be empty.",
    });
    return;
  }

  if (to === sender) {
    sendJSON(ws, {
      type: "error",
      message: "You cannot send a message to yourself.",
    });
    return;
  }

  const recipientSocket = clients.get(to);

  if (!recipientSocket || recipientSocket.readyState !== WebSocket.OPEN) {
    console.log(
      `[BROKER] Route FAIL  : '${sender}' -> '${to}' (user offline)`
    );
    sendJSON(ws, {
      type: "error",
      message: `User '${to}' is not online.`,
    });
    return;
  }

  // ── Deliver as plain text to the recipient ─────────────────────────────────
  //    The recipient receives a clean, human-readable string:
  //        "From alice: hello!"
  //    No JSON wrapper – no parsing needed on the receiving end.
  const plainDelivery = `From ${sender}: ${body}`;
  sendPlainText(recipientSocket, plainDelivery);

  console.log(
    `[BROKER] Routed      : '${sender}' -> '${to}'  |  "${body}"`
  );

  // Acknowledge delivery to sender only (JSON control, not seen by recipient)
  sendJSON(ws, {
    type: "system",
    message: `Message delivered to '${to}'.`,
  });
}

// ─── Handler: unknown type ─────────────────────────────────────────────────────

function handleUnknown(ws, type) {
  sendJSON(ws, {
    type: "error",
    message: `Unknown message type: '${type}'.`,
  });
}

// ─── Core connection handler ───────────────────────────────────────────────────

function onConnection(ws, req) {
  const ip = req.socket.remoteAddress;
  console.log(`[BROKER] Connected   : ${ip}`);

  // Clients must register within 10 s or the broker drops the connection
  const registrationTimeout = setTimeout(() => {
    if (!ws._brokerName) {
      console.log(
        `[BROKER] Timeout     : ${ip} did not register – closing.`
      );
      sendJSON(ws, {
        type: "error",
        message: "Registration timeout. Closing connection.",
      });
      ws.terminate();
    }
  }, 10_000);

  // ── Incoming frame ─────────────────────────────────────────────────────────
  ws.on("message", (raw) => {
    let payload;

    try {
      payload = JSON.parse(raw.toString());
    } catch {
      sendJSON(ws, { type: "error", message: "Invalid JSON payload." });
      return;
    }

    const type = (payload.type || "").toLowerCase();

    switch (type) {
      case "register":
        clearTimeout(registrationTimeout);
        handleRegister(ws, payload);
        break;

      case "message":
        handleMessage(ws, payload);
        break;

      default:
        handleUnknown(ws, type);
    }
  });

  // ── Disconnect ─────────────────────────────────────────────────────────────
  ws.on("close", () => {
    clearTimeout(registrationTimeout);
    const name = ws._brokerName;
    if (name) {
      clients.delete(name);
      console.log(
        `[BROKER] Disconnected: '${name}'  (active users: ${clients.size})`
      );
    } else {
      console.log(`[BROKER] Disconnected: ${ip} (was not registered)`);
    }
  });

  // ── Socket-level errors ────────────────────────────────────────────────────
  ws.on("error", (err) => {
    const label = ws._brokerName || ip;
    console.error(`[BROKER] Socket error: '${label}'  ->  ${err.message}`);
  });
}

// ─── Start broker ──────────────────────────────────────────────────────────────

const wss = new WebSocket.Server({ port: PORT }, () => {
  console.log("+===========================================+");
  console.log(`|  WebSocket Broker  –  listening on :${PORT}  |`);
  console.log("+===========================================+");
  console.log(`[BROKER] ws://localhost:${PORT}`);
  console.log("[BROKER] Waiting for clients...\n");
});

wss.on("connection", onConnection);

// ─── Server-level errors ───────────────────────────────────────────────────────

wss.on("error", (err) => {
  console.error("[BROKER] Server error:", err.message);
  process.exit(1);
});

// ─── Graceful shutdown ─────────────────────────────────────────────────────────

function shutdown(signal) {
  console.log(`\n[BROKER] ${signal} – shutting down gracefully…`);

  clients.forEach((ws, name) => {
    sendJSON(ws, { type: "system", message: "Broker is shutting down." });
    ws.terminate();
    console.log(`[BROKER] Terminated  : '${name}'`);
  });

  clients.clear();

  wss.close(() => {
    console.log("[BROKER] Closed. Goodbye.");
    process.exit(0);
  });
}

process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
