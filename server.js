const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");
});

const wss = new WebSocketServer({ server });

// roomId -> Map(userId -> { ws, userName })
const rooms = new Map();

function safeSend(ws, obj) {
  try {
    if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
  } catch (_) {}
}

function broadcast(roomId, obj, exceptWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  for (const { ws } of room.values()) {
    if (exceptWs && ws === exceptWs) continue;
    safeSend(ws, obj);
  }
}

function getUsersPayload(roomId) {
  const room = rooms.get(roomId);
  const users = [];
  if (room) {
    for (const [userId, data] of room.entries()) {
      users.push({ userId: String(userId), userName: data.userName || "" });
    }
  }
  return users;
}

function sendRoomUsers(roomId) {
  broadcast(roomId, { type: "room-users", roomId, users: getUsersPayload(roomId) });
}

function cleanup(ws) {
  const roomId = ws._roomId;
  const userId = ws._userId;
  const userName = ws._userName;

  if (!roomId || !userId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  room.delete(String(userId));

  if (room.size === 0) {
    rooms.delete(roomId);
    return;
  }

  broadcast(roomId, { type: "user-left", roomId, userId: String(userId), userName: userName || "" }, ws);
  sendRoomUsers(roomId);
}

// ping co 25s (żeby nie zrywało WS przez proxy)
setInterval(() => {
  for (const ws of wss.clients) {
    try { ws.ping(); } catch (_) {}
  }
}, 25000);

wss.on("connection", (ws) => {
  ws.on("message", (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch (_) { return; }

    const type = msg?.type;

    if (type === "join") {
      const roomId = String(msg.roomId || "").trim();
      const userId = String(msg.userId || "").trim();
      const userName = String(msg.userName || "").trim();
      if (!roomId || !userId) return;

      ws._roomId = roomId;
      ws._userId = userId;
      ws._userName = userName;

      if (!rooms.has(roomId)) rooms.set(roomId, new Map());
      const room = rooms.get(roomId);

      // wyślij nowemu istniejących użytkowników jako user-joined
      for (const [existingId, existing] of room.entries()) {
        safeSend(ws, { type: "user-joined", roomId, userId: String(existingId), userName: existing.userName || "" });
      }

      room.set(userId, { ws, userName });

      // powiadom pozostałych
      broadcast(roomId, { type: "user-joined", roomId, userId, userName }, ws);
      sendRoomUsers(roomId);
      return;
    }

    // routing offer/answer/ice do konkretnego usera
    const roomId = ws._roomId;
    const fromUserId = ws._userId;
    const fromUserName = ws._userName || "";
    if (!roomId || !fromUserId) return;

    const room = rooms.get(roomId);
    if (!room) return;

    const targetUserId = msg.targetUserId ? String(msg.targetUserId) : null;
    if (!targetUserId) return;

    const target = room.get(targetUserId);
    if (!target?.ws) return;

    if (type === "offer") {
      safeSend(target.ws, { type: "offer", roomId, fromUserId, fromUserName, offer: msg.offer });
    } else if (type === "answer") {
      safeSend(target.ws, { type: "answer", roomId, fromUserId, fromUserName, answer: msg.answer });
    } else if (type === "ice-candidate") {
      safeSend(target.ws, { type: "ice-candidate", roomId, fromUserId, fromUserName, candidate: msg.candidate });
    }
  });

  ws.on("close", () => cleanup(ws));
  ws.on("error", () => cleanup(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Signaling server listening on:", PORT);
});
