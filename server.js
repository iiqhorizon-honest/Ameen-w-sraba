// Stay Focused — minimal WebSocket relay server
// This server holds NO game logic. It only routes messages between
// players in a room so the game works across any networks (no P2P/NAT needed).

const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Stay Focused relay server is running.\n');
});

const wss = new WebSocket.Server({ server });

// code -> { hostId, members: Map(id -> ws) }
const rooms = {};

function genId() {
  return Math.random().toString(36).slice(2, 10);
}

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

function cleanupSocket(ws) {
  const code = ws.roomCode;
  if (!code || !rooms[code]) return;
  const room = rooms[code];
  room.members.delete(ws.id);

  if (ws.id === room.hostId) {
    // Host left — the room can't continue without its referee.
    room.members.forEach((memberWs) => safeSend(memberWs, { type: 'hostLeft' }));
    delete rooms[code];
  } else if (room.members.size === 0) {
    delete rooms[code];
  }
}

wss.on('connection', (ws) => {
  ws.id = genId();
  ws.roomCode = null;
  ws.isAlive = true;

  safeSend(ws, { type: 'welcome', id: ws.id });

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'create': {
        const code = String(msg.code || '').toUpperCase();
        if (!code) return;
        if (rooms[code]) { safeSend(ws, { type: 'createResult', ok: false, reason: 'taken' }); return; }
        rooms[code] = { hostId: ws.id, members: new Map([[ws.id, ws]]) };
        ws.roomCode = code;
        safeSend(ws, { type: 'createResult', ok: true });
        break;
      }
      case 'joinRoom': {
        const code = String(msg.code || '').toUpperCase();
        const room = rooms[code];
        if (!room) { safeSend(ws, { type: 'joinResult', ok: false, reason: 'notfound' }); return; }
        room.members.set(ws.id, ws);
        ws.roomCode = code;
        safeSend(ws, { type: 'joinResult', ok: true });
        break;
      }
      case 'toHost': {
        const room = rooms[String(msg.code || '').toUpperCase()];
        if (!room) return;
        const hostWs = room.members.get(room.hostId);
        safeSend(hostWs, { type: 'relay', from: ws.id, payload: msg.payload });
        break;
      }
      case 'toClient': {
        const room = rooms[String(msg.code || '').toUpperCase()];
        if (!room) return;
        const target = room.members.get(msg.targetId);
        safeSend(target, { type: 'relay', from: ws.id, payload: msg.payload });
        break;
      }
      case 'broadcast': {
        const room = rooms[String(msg.code || '').toUpperCase()];
        if (!room) return;
        room.members.forEach((memberWs, memberId) => {
          if (memberId !== ws.id) safeSend(memberWs, { type: 'relay', from: ws.id, payload: msg.payload });
        });
        break;
      }
    }
  });

  ws.on('close', () => cleanupSocket(ws));
  ws.on('error', () => cleanupSocket(ws));
});

// Keep connections alive / detect dead sockets
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Relay server listening on port ' + PORT));
