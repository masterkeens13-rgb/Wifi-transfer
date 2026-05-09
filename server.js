const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = path.join(__dirname, "received_files");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === "/" || url.pathname === "/client.html") {
    const filePath = path.join(__dirname, "client.html");
    if (!fs.existsSync(filePath)) { res.writeHead(404); return res.end("client.html not found"); }
    res.writeHead(200, { "Content-Type": "text/html" });
    return res.end(fs.readFileSync(filePath));
  }
  if (url.pathname === "/download") {
    const filename = url.searchParams.get("file");
    if (!filename) { res.writeHead(400); return res.end("Missing file"); }
    const safe = path.basename(filename);
    const filePath = path.join(UPLOAD_DIR, safe);
    if (!fs.existsSync(filePath)) { res.writeHead(404); return res.end("Not found"); }
    res.writeHead(200, { "Content-Disposition": `attachment; filename="${safe}"`, "Content-Type": "application/octet-stream", "Content-Length": fs.statSync(filePath).size });
    return fs.createReadStream(filePath).pipe(res);
  }
  res.writeHead(404); res.end("Not found");
});

const wss = new WebSocketServer({ server: httpServer });
const clients = new Map();
let nextId = 1;

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  for (const [id, client] of clients)
    if (id !== excludeId && client.ws.readyState === WebSocket.OPEN) client.ws.send(msg);
}
function sendTo(id, data) {
  const client = clients.get(id);
  if (client && client.ws.readyState === WebSocket.OPEN) client.ws.send(JSON.stringify(data));
}
function clientList() { return [...clients.entries()].map(([id, c]) => ({ id, name: c.name })); }

wss.on("connection", (ws, req) => {
  const id = nextId++;
  clients.set(id, { ws, name: `Device-${id}` });
  console.log(`[+] Client #${id} connected`);
  ws.send(JSON.stringify({ type: "welcome", yourId: id, clients: clientList(), serverName: "Cloud Server" }));
  broadcast({ type: "client_joined", client: { id, name: `Device-${id}` } }, id);

  ws.on("message", (raw, isBinary) => {
    if (isBinary) {
      const nullIdx = raw.indexOf(0);
      if (nullIdx === -1) return;
      let meta;
      try { meta = JSON.parse(raw.slice(0, nullIdx).toString("utf8")); } catch { return; }
      const fileData = raw.slice(nullIdx + 1);
      const safe = path.basename(meta.filename || "upload");
      fs.writeFileSync(path.join(UPLOAD_DIR, safe), fileData);
      console.log(`[FILE] "${safe}" (${(fileData.length/1024).toFixed(1)} KB)`);
      ws.send(JSON.stringify({ type: "file_received", filename: safe, size: fileData.length, downloadUrl: `/download?file=${encodeURIComponent(safe)}` }));
      const notify = { type: "incoming_file", fromId: id, fromName: clients.get(id).name, filename: safe, size: fileData.length, downloadUrl: `/download?file=${encodeURIComponent(safe)}` };
      if (meta.toId && clients.has(meta.toId)) sendTo(meta.toId, notify);
      else broadcast(notify, id);
      return;
    }
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    switch (msg.type) {
      case "set_name":
        const newName = String(msg.name||"").trim().slice(0,32)||`Device-${id}`;
        clients.get(id).name = newName;
        broadcast({ type: "client_renamed", id, name: newName });
        break;
      case "chat":
        const text = String(msg.text||"").slice(0,4000);
        const fromName = clients.get(id).name;
        if (msg.toId && clients.has(msg.toId)) {
          sendTo(msg.toId, { type: "chat", fromId: id, fromName, text, private: true });
          ws.send(JSON.stringify({ type: "chat_sent", toId: msg.toId, text, private: true }));
        } else {
          broadcast({ type: "chat", fromId: id, fromName, text }, id);
          ws.send(JSON.stringify({ type: "chat_sent", text }));
        }
        break;
      case "ping": ws.send(JSON.stringify({ type: "pong" })); break;
    }
  });
  ws.on("close", () => { const name = clients.get(id)?.name; clients.delete(id); broadcast({ type: "client_left", id, name }); });
  ws.on("error", err => console.error(`[ERR] #${id}:`, err.message));
});

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
