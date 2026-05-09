const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = path.join(__dirname, "received_files");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const rooms = new Map();
const clients = new Map();
let nextId = 1;

function generatePin() { return Math.floor(100000 + Math.random() * 900000).toString(); }

function getRoom(name) { return rooms.get(name); }

function createRoom(name, isPublic, pin = null) {
  const room = { name, isPublic, pin, clients: new Set(), files: [], chat: [] };
  rooms.set(name, room);
  return room;
}

function broadcastToRoom(roomName, data, excludeId = null) {
  const room = rooms.get(roomName);
  if (!room) return;
  const msg = JSON.stringify(data);
  for (const cid of room.clients) {
    if (cid === excludeId) continue;
    const client = clients.get(cid);
    if (client && client.ws.readyState === WebSocket.OPEN) client.ws.send(msg);
  }
}

function broadcastAll(data) {
  const msg = JSON.stringify(data);
  for (const [, client] of clients)
    if (client.ws.readyState === WebSocket.OPEN) client.ws.send(msg);
}

function publicRoomList() {
  return [...rooms.values()]
    .filter(r => r.isPublic)
    .map(r => ({ name: r.name, members: r.clients.size, files: r.files.length }));
}

function globalVaultFiles() {
  const all = [];
  for (const room of rooms.values()) {
    if (room.isPublic) all.push(...room.files.map(f => ({ ...f, room: room.name })));
  }
  return all.sort((a, b) => b.ts - a.ts);
}

const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/" || url.pathname === "/client.html") {
    const filePath = path.join(__dirname, "client.html");
    if (!fs.existsSync(filePath)) { res.writeHead(404); return res.end("client.html not found"); }
    res.writeHead(200, { "Content-Type": "text/html" });
    return res.end(fs.readFileSync(filePath));
  }

  if (url.pathname === "/rooms") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(publicRoomList()));
  }

  if (url.pathname === "/vault") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(globalVaultFiles()));
  }

  if (url.pathname === "/download") {
    const filename = url.searchParams.get("file");
    if (!filename) { res.writeHead(400); return res.end("Missing file"); }
    const safe = path.basename(filename);
    const filePath = path.join(UPLOAD_DIR, safe);
    if (!fs.existsSync(filePath)) { res.writeHead(404); return res.end("Not found"); }
    res.writeHead(200, {
      "Content-Disposition": `attachment; filename="${safe}"`,
      "Content-Type": "application/octet-stream",
      "Content-Length": fs.statSync(filePath).size
    });
    return fs.createReadStream(filePath).pipe(res);
  }

  res.writeHead(404); res.end("Not found");
});

const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws) => {
  const id = nextId++;
  clients.set(id, { ws, name: `Device-${id}`, room: null });
  console.log(`[+] Client #${id} connected`);

  ws.send(JSON.stringify({ type: "welcome", yourId: id, rooms: publicRoomList() }));

  ws.on("message", (raw, isBinary) => {
    if (isBinary) {
      const nullIdx = raw.indexOf(0);
      if (nullIdx === -1) return;
      let meta;
      try { meta = JSON.parse(raw.slice(0, nullIdx).toString("utf8")); } catch { return; }
      const client = clients.get(id);
      if (!client || !client.room) return;
      const room = rooms.get(client.room);
      if (!room) return;

      const fileData = raw.slice(nullIdx + 1);
      const safe = path.basename(meta.filename || "upload");
      const savePath = path.join(UPLOAD_DIR, safe);
      fs.writeFileSync(savePath, fileData);

      const fileEntry = {
        filename: safe, size: fileData.length,
        fromId: id, fromName: client.name,
        downloadUrl: `/download?file=${encodeURIComponent(safe)}`,
        ts: Date.now()
      };
      room.files.push(fileEntry);

      console.log(`[FILE] "${safe}" in room "${client.room}"`);

      ws.send(JSON.stringify({ type: "file_received", ...fileEntry }));

      if (meta.toId && clients.has(meta.toId)) {
        clients.get(meta.toId).ws.send(JSON.stringify({ type: "incoming_file", ...fileEntry }));
      } else {
        broadcastToRoom(client.room, { type: "incoming_file", ...fileEntry }, id);
      }

      if (room.isPublic) broadcastAll({ type: "vault_update", files: globalVaultFiles() });
      return;
    }

    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    const client = clients.get(id);

    switch (msg.type) {
      case "set_name": {
        const newName = String(msg.name || "").trim().slice(0, 32) || `Device-${id}`;
        client.name = newName;
        if (client.room) broadcastToRoom(client.room, { type: "client_renamed", id, name: newName }, id);
        break;
      }
      case "create_room": {
        const rname = String(msg.name || "").trim().slice(0, 32);
        if (!rname) { ws.send(JSON.stringify({ type: "error", msg: "Room name required" })); break; }
        if (rooms.has(rname)) { ws.send(JSON.stringify({ type: "error", msg: "Room already exists" })); break; }
        const isPublic = msg.isPublic !== false;
        const pin = isPublic ? null : generatePin();
        const room = createRoom(rname, isPublic, pin);
        room.clients.add(id);
        client.room = rname;
        ws.send(JSON.stringify({ type: "room_joined", room: rname, isPublic, pin, members: [{ id, name: client.name }], files: room.files }));
        if (isPublic) broadcastAll({ type: "rooms_update", rooms: publicRoomList() });
        console.log(`[ROOM] Created "${rname}" (${isPublic ? "public" : "private"})`);
        break;
      }
      case "join_room": {
        const rname = String(msg.name || "").trim();
        const room = rooms.get(rname);
        if (!room) { ws.send(JSON.stringify({ type: "error", msg: "Room not found" })); break; }
        if (!room.isPublic && room.pin !== String(msg.pin || "")) {
          ws.send(JSON.stringify({ type: "error", msg: "Wrong PIN" })); break;
        }
        if (client.room) {
          const oldRoom = rooms.get(client.room);
          if (oldRoom) { oldRoom.clients.delete(id); broadcastToRoom(client.room, { type: "client_left", id, name: client.name }); }
        }
        room.clients.add(id);
        client.room = rname;
        const members = [...room.clients].map(cid => ({ id: cid, name: clients.get(cid)?.name || "?" }));
        ws.send(JSON.stringify({ type: "room_joined", room: rname, isPublic: room.isPublic, members, files: room.files }));
        broadcastToRoom(rname, { type: "client_joined", client: { id, name: client.name } }, id);
        break;
      }
      case "leave_room": {
        const room = rooms.get(client.room);
        if (room) {
          room.clients.delete(id);
          broadcastToRoom(client.room, { type: "client_left", id, name: client.name });
          if (room.clients.size === 0) { rooms.delete(client.room); broadcastAll({ type: "rooms_update", rooms: publicRoomList() }); }
        }
        client.room = null;
        ws.send(JSON.stringify({ type: "left_room", rooms: publicRoomList() }));
        break;
      }
      case "chat": {
        if (!client.room) break;
        const text = String(msg.text || "").slice(0, 4000);
        if (msg.toId && clients.has(msg.toId)) {
          clients.get(msg.toId).ws.send(JSON.stringify({ type: "chat", fromId: id, fromName: client.name, text, private: true }));
          ws.send(JSON.stringify({ type: "chat_sent", text, private: true }));
        } else {
          broadcastToRoom(client.room, { type: "chat", fromId: id, fromName: client.name, text }, id);
          ws.send(JSON.stringify({ type: "chat_sent", text }));
        }
        break;
      }
      case "get_vault":
        ws.send(JSON.stringify({ type: "vault_update", files: globalVaultFiles() }));
        break;
      case "get_rooms":
        ws.send(JSON.stringify({ type: "rooms_update", rooms: publicRoomList() }));
        break;
    }
  });

  ws.on("close", () => {
    const client = clients.get(id);
    if (client?.room) {
      const room = rooms.get(client.room);
      if (room) {
        room.clients.delete(id);
        broadcastToRoom(client.room, { type: "client_left", id, name: client.name });
        if (room.clients.size === 0) { rooms.delete(client.room); broadcastAll({ type: "rooms_update", rooms: publicRoomList() }); }
      }
    }
    clients.delete(id);
    console.log(`[-] Client #${id} disconnected`);
  });

  ws.on("error", err => console.error(`[ERR] #${id}:`, err.message));
});

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
