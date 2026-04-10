#!/usr/bin/env node

/**
 * server.js — LAN File Transfer (Node.js rewrite)
 *
 * Zero external dependencies. Uses built-in modules only.
 * Protocol-compatible with the original Rust version:
 *   - UDP discovery on port 34254
 *   - TCP file transfer on port 34255
 *   - XOR obfuscation with key "LAN-XFER-KEY-2024"
 *
 * Run:  node server.js
 * Then open http://localhost:3000 in a browser.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const dgram = require('dgram');
const net = require('net');
const os = require('os');
const { exec } = require('child_process');

// ──────────────────────────────────────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────────────────────────────────────
const DISCOVERY_PORT = 34254;
const TRANSFER_PORT = 34255;
const HTTP_PORT = 3000;
const XOR_KEY = Buffer.from('LAN-XFER-KEY-2024');
const PEER_TIMEOUT = 10_000;   // ms
const BROADCAST_INTERVAL = 3_000; // ms

// ──────────────────────────────────────────────────────────────────────────────
// Persistent config (device name)
// ──────────────────────────────────────────────────────────────────────────────
const CONFIG_PATH = path.join(os.homedir(), '.lantransfer.json');

function loadConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf-8'));
  } catch { return {}; }
}

function saveConfig(cfg) {
  try {
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));
    console.log(`  [config] Saved to ${CONFIG_PATH}`);
  } catch (e) {
    console.error(`  [config] Failed to save: ${e.message}`);
  }
}

const config = loadConfig();

// ──────────────────────────────────────────────────────────────────────────────
// Application state
// ──────────────────────────────────────────────────────────────────────────────
let isDiscoverable = true;
let deviceName = config.deviceName || os.hostname();
let downloadDir = config.downloadDir || path.join(os.homedir(), 'Downloads');
const sharedFiles = [];               // [ { id, path, name, size } ]
const peers = new Map();              // id → { device_name, ip, tcp_port, last_seen }
const transfers = [];                 // [ TransferEntry ]
const pendingRequests = new Map();    // id → { filename, size, senderName, resolve }
let idCounter = 0;
let browseInProgress = false;

// ──────────────────────────────────────────────────────────────────────────────
// SSE (Server-Sent Events) clients
// ──────────────────────────────────────────────────────────────────────────────
const sseClients = new Set();

function getLocalIP() {
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const iface of ifaces) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  return '127.0.0.1';
}

/** Compute subnet broadcast address from local IP and netmask. */
function getBroadcastAddresses() {
  const addrs = [];
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const iface of ifaces) {
      if (iface.family === 'IPv4' && !iface.internal && iface.netmask) {
        const ipParts = iface.address.split('.').map(Number);
        const maskParts = iface.netmask.split('.').map(Number);
        const broadcast = ipParts.map((octet, i) => (octet | (~maskParts[i] & 255))).join('.');
        addrs.push(broadcast);
      }
    }
  }
  return addrs.length > 0 ? addrs : ['255.255.255.255'];
}

function serializeState() {
  return JSON.stringify({
    isDiscoverable,
    deviceName,
    downloadDir,
    localIP: getLocalIP(),
    peers: [...peers.entries()].map(([id, p]) => ({
      id, name: p.device_name, ip: p.ip,
    })),
    sharedFiles: sharedFiles.map(f => ({ id: f.id, name: f.name, size: f.size })),
    selectedFileId: sharedFiles.length > 0 ? sharedFiles[sharedFiles.length - 1].id : null,
    transfers: transfers.map(t => ({
      id: t.id, filename: t.filename, size: t.size,
      direction: t.direction, peerName: t.peerName,
      status: t.status, progress: t.progress,
      speed: t.speed, error: t.error,
      savePath: t.savePath || null,
    })),
    incomingRequests: [...pendingRequests.entries()].map(([id, r]) => ({
      id, filename: r.filename, size: r.size, senderName: r.senderName,
    })),
  });
}

function broadcast() {
  const msg = `data: ${serializeState()}\n\n`;
  for (const res of sseClients) {
    try { res.write(msg); } catch { /* client gone */ }
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────
function xorCrypt(buf, offset) {
  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i < buf.length; i++) {
    out[i] = buf[i] ^ XOR_KEY[(offset + i) % XOR_KEY.length];
  }
  return out;
}

function readBody(req) {
  return new Promise(resolve => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => resolve(body));
  });
}

/** Read exactly `n` bytes from a socket. */
function readExact(socket, n) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    const onData = chunk => {
      buf = Buffer.concat([buf, chunk]);
      if (buf.length >= n) {
        cleanup();
        if (buf.length > n) socket.unshift(buf.subarray(n));
        resolve(buf.subarray(0, n));
      }
    };
    const onErr = err => { cleanup(); reject(err); };
    const onEnd = () => { cleanup(); reject(new Error('Disconnected')); };
    const onClose = () => { cleanup(); reject(new Error('Connection closed')); };
    function cleanup() {
      socket.off('data', onData);
      socket.off('error', onErr);
      socket.off('end', onEnd);
      socket.off('close', onClose);
    }
    socket.on('data', onData);
    socket.on('error', onErr);
    socket.on('end', onEnd);
    socket.on('close', onClose);
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP server  (serves UI + SSE + API)
// ──────────────────────────────────────────────────────────────────────────────
const htmlPath = path.join(__dirname, 'public', 'index.html');

const server = http.createServer(async (req, res) => {
  // ── Serve the web UI ──
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    fs.readFile(htmlPath, (err, data) => {
      if (err) { res.writeHead(500); res.end('Error loading UI'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // ── Ping endpoint for HTTP-based discovery ──
  if (req.method === 'GET' && req.url === '/api/ping') {
    if (!isDiscoverable) {
      res.writeHead(403);
      res.end('Hidden');
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(JSON.stringify({
      app: 'lantransfer',
      device_name: deviceName,
      tcp_port: TRANSFER_PORT,
    }));
    return;
  }

  // ── SSE stream ──
  if (req.method === 'GET' && req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
    res.write(`data: ${serializeState()}\n\n`);
    return;
  }

  // ── API endpoints ──
  if (req.method === 'POST') {
    const body = await readBody(req);
    let data = {};
    try { data = JSON.parse(body); } catch { /* empty body is fine */ }

    if (req.url === '/api/toggle') {
      isDiscoverable = !isDiscoverable;
      console.log(`  [toggle] Discoverable: ${isDiscoverable}`);
    }
    else if (req.url === '/api/browse') {
      if (browseInProgress) {
        console.log('  [browse] Dialog already open, ignoring');
      } else {
        browseInProgress = true;
        console.log('  [browse] Opening file dialog...');
        const fp = await openFileDialog();
        browseInProgress = false;
        if (fp) {
          try {
            const s = fs.statSync(fp);
            const file = { id: String(idCounter++), path: fp, name: path.basename(fp), size: s.size };
            sharedFiles.push(file);
            console.log(`  [browse] Added: ${file.name} (${formatSize(file.size)})`);
          } catch (e) {
            console.error(`  [browse] Error reading file: ${e.message}`);
          }
        } else {
          console.log('  [browse] Cancelled');
        }
      }
    }
    else if (req.url === '/api/remove-file') {
      const idx = sharedFiles.findIndex(f => f.id === data.fileId);
      if (idx !== -1) {
        console.log(`  [files] Removed: ${sharedFiles[idx].name}`);
        sharedFiles.splice(idx, 1);
      }
    }
    else if (req.url === '/api/send') {
      const peer = peers.get(data.peerId);
      const file = sharedFiles.find(f => f.id === data.fileId);
      if (peer && file) {
        console.log(`  [send] ${file.name} -> ${peer.device_name} (${peer.ip})`);
        sendFile(peer, { ...file });
      } else {
        console.log(`  [send] Failed — peer: ${!!peer}, file: ${!!file}`);
      }
    }
    else if (req.url === '/api/rename') {
      const newName = (data.name || '').trim();
      if (newName && newName.length <= 50) {
        deviceName = newName;
        config.deviceName = newName;
        saveConfig(config);
        console.log(`  [rename] Device name set to: ${deviceName}`);
      }
    }
    else if (req.url === '/api/set-download-dir') {
      // Open native folder picker
      const dir = await openFolderDialog();
      if (dir) {
        downloadDir = dir;
        config.downloadDir = dir;
        saveConfig(config);
        console.log(`  [config] Download dir set to: ${downloadDir}`);
      }
    }
    else if (req.url === '/api/respond') {
      const pending = pendingRequests.get(data.requestId);
      if (pending) {
        pending.resolve(!!data.accepted);
        pendingRequests.delete(data.requestId);
        console.log(`  [respond] ${data.accepted ? 'Accepted' : 'Rejected'} transfer ${data.requestId}`);
      }
    }
    else if (req.url === '/api/open-folder') {
      const filePath = data.path;
      if (filePath && fs.existsSync(filePath)) {
        const dir = path.dirname(filePath);
        if (process.platform === 'win32') exec(`explorer /select,"${filePath}"`);
        else if (process.platform === 'darwin') exec(`open -R "${filePath}"`);
        else exec(`xdg-open "${dir}"`);
        console.log(`  [open] ${filePath}`);
      }
    }
    else if (req.url === '/api/shutdown') {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      console.log('\n  Server stopped by user.');
      setTimeout(() => process.exit(0), 200);
      return;
    }

    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    broadcast();
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

// ──────────────────────────────────────────────────────────────────────────────
// Native file dialog
// ──────────────────────────────────────────────────────────────────────────────
function openFileDialog() {
  return new Promise(resolve => {
    let cmd;
    if (process.platform === 'win32') {
      cmd = 'powershell -sta -command "Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.OpenFileDialog; $f.Title = \'Select file to send\'; if($f.ShowDialog() -eq \'OK\'){Write-Output $f.FileName}"';
    } else if (process.platform === 'darwin') {
      cmd = 'osascript -e \'POSIX path of (choose file with prompt "Select file to send")\'';
    } else {
      cmd = 'zenity --file-selection --title="Select file to send" 2>/dev/null || kdialog --getopenfilename . 2>/dev/null';
    }
    exec(cmd, { encoding: 'utf-8', windowsHide: true, timeout: 120_000 }, (err, stdout) => {
      resolve(err ? null : (stdout.trim() || null));
    });
  });
}

function openFolderDialog() {
  return new Promise(resolve => {
    let cmd;
    if (process.platform === 'win32') {
      cmd = 'powershell -sta -command "Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.FolderBrowserDialog; $f.Description = \'Select download folder\'; if($f.ShowDialog() -eq \'OK\'){Write-Output $f.SelectedPath}"';
    } else if (process.platform === 'darwin') {
      cmd = 'osascript -e \'POSIX path of (choose folder with prompt "Select download folder")\'';
    } else {
      cmd = 'zenity --file-selection --directory --title="Select download folder" 2>/dev/null || kdialog --getexistingdirectory . 2>/dev/null';
    }
    exec(cmd, { encoding: 'utf-8', windowsHide: true, timeout: 120_000 }, (err, stdout) => {
      resolve(err ? null : (stdout.trim() || null));
    });
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// UDP discovery
// ──────────────────────────────────────────────────────────────────────────────
const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true });

udp.on('listening', () => {
  udp.setBroadcast(true);
});

udp.on('message', (msg, rinfo) => {
  try {
    const d = JSON.parse(msg.toString());
    if (d.device_name === deviceName) return; // ignore self
    const id = `${rinfo.address}:${d.tcp_port}`;
    const isNew = !peers.has(id);
    peers.set(id, {
      device_name: d.device_name,
      ip: rinfo.address,
      tcp_port: d.tcp_port,
      last_seen: Date.now(),
    });
    if (isNew) broadcast();
  } catch { /* ignore malformed packets */ }
});

udp.on('error', err => {
  console.error(`  UDP error: ${err.message}`);
  console.error('  Is another instance running? (port conflict on UDP ' + DISCOVERY_PORT + ')');
});

udp.bind(DISCOVERY_PORT);

// Broadcaster — send beacon every 3 s (to subnet broadcast addresses)
setInterval(() => {
  if (!isDiscoverable) return;
  const msg = JSON.stringify({ device_name: deviceName, tcp_port: TRANSFER_PORT });
  for (const addr of getBroadcastAddresses()) {
    udp.send(msg, DISCOVERY_PORT, addr, () => {});
  }
}, BROADCAST_INTERVAL);

// Prune stale peers
setInterval(() => {
  let changed = false;
  for (const [id, p] of peers) {
    if (Date.now() - p.last_seen > PEER_TIMEOUT) { peers.delete(id); changed = true; }
  }
  if (changed) broadcast();
}, 2000);

// ──────────────────────────────────────────────────────────────────────────────
// TCP file transfer — receiver
// ──────────────────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
  handleIncoming(socket).catch(err => {
    console.error(`  Receive error: ${err.message}`);
  });
});

tcpServer.on('error', err => {
  console.error(`  TCP error: ${err.message}`);
  console.error('  Is another instance running? (port conflict on TCP ' + TRANSFER_PORT + ')');
});

tcpServer.listen(TRANSFER_PORT);

async function handleIncoming(socket) {
  // Read header length (8 bytes, little-endian u64)
  const lenBuf = await readExact(socket, 8);
  const headerLen = Number(lenBuf.readBigUInt64LE(0));

  // Read header JSON
  const headerBuf = await readExact(socket, headerLen);
  const header = JSON.parse(headerBuf.toString('utf-8'));
  const { filename, size, sender_name } = header;

  console.log(`  Incoming: ${filename} (${formatSize(size)}) from ${sender_name}`);

  // Prompt user for accept / reject
  const reqId = String(idCounter++);
  const accepted = await new Promise(resolve => {
    pendingRequests.set(reqId, { filename, size, senderName: sender_name, resolve });
    broadcast();

    // Auto-reject if sender disconnects while waiting
    socket.once('close', () => {
      if (pendingRequests.has(reqId)) {
        pendingRequests.delete(reqId);
        resolve(false);
        broadcast();
      }
    });
  });

  // Send decision byte
  socket.write(Buffer.from([accepted ? 1 : 0]));
  if (!accepted) { socket.end(); return; }

  // Create transfer entry
  const tid = String(idCounter++);
  const t = {
    id: tid, filename, size, direction: 'receive', peerName: sender_name,
    status: 'in_progress', progress: 0, speed: 0, error: null,
    _start: Date.now(), _bytes: 0,
  };
  transfers.push(t);
  broadcast();

  // Unique save path in download directory
  const dl = downloadDir;
  if (!fs.existsSync(dl)) fs.mkdirSync(dl, { recursive: true });
  let savePath = path.join(dl, filename);
  let counter = 1;
  const ext = path.extname(filename);
  const base = path.basename(filename, ext);
  while (fs.existsSync(savePath)) {
    savePath = path.join(dl, `${base} (${counter++})${ext}`);
  }

  t.savePath = savePath;

  const ws = fs.createWriteStream(savePath);
  let received = 0;
  let lastBc = 0;

  socket.on('data', chunk => {
    const decrypted = xorCrypt(chunk, received);
    ws.write(decrypted);
    received += chunk.length;

    const now = Date.now();
    if (now - lastBc > 300 || received >= size) {
      t._bytes = received;
      t.progress = Math.min(100, Math.round(received / size * 100));
      const elapsed = (now - t._start) / 1000;
      t.speed = elapsed > 0 ? received / elapsed : 0;
      broadcast();
      lastBc = now;
    }
  });

  await new Promise((resolve, reject) => {
    socket.on('end', () => {
      ws.end();
      t.status = 'completed';
      t.progress = 100;
      broadcast();
      console.log(`  Saved: ${savePath}`);
      resolve();
    });
    socket.on('error', err => {
      ws.end();
      t.status = 'failed';
      t.error = err.message;
      broadcast();
      reject(err);
    });
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// TCP file transfer — sender
// ──────────────────────────────────────────────────────────────────────────────
async function sendFile(peer, file) {
  const tid = String(idCounter++);
  const t = {
    id: tid, filename: file.name, size: file.size, direction: 'send',
    peerName: peer.device_name, status: 'waiting', progress: 0, speed: 0,
    error: null, _start: Date.now(), _bytes: 0,
  };
  transfers.push(t);
  broadcast();

  try {
    const socket = new net.Socket();
    await new Promise((res, rej) => {
      socket.connect(peer.tcp_port, peer.ip, res);
      socket.once('error', rej);
    });

    // Send header
    const headerJSON = JSON.stringify({
      filename: file.name, size: file.size, sender_name: deviceName,
    });
    const headerBuf = Buffer.from(headerJSON, 'utf-8');
    const lenBuf = Buffer.alloc(8);
    lenBuf.writeBigUInt64LE(BigInt(headerBuf.length));
    socket.write(lenBuf);
    socket.write(headerBuf);

    // Wait for accept/reject
    const resp = await readExact(socket, 1);
    if (resp[0] !== 1) {
      t.status = 'failed';
      t.error = 'Rejected by recipient';
      broadcast();
      socket.end();
      return;
    }

    t.status = 'in_progress';
    t._start = Date.now();
    broadcast();

    // Stream file with XOR encryption
    const rs = fs.createReadStream(file.path, { highWaterMark: 65536 });
    let sent = 0;
    let lastBc = 0;

    for await (const chunk of rs) {
      const encrypted = xorCrypt(Buffer.from(chunk), sent);
      const ok = socket.write(encrypted);
      sent += chunk.length;

      // Backpressure
      if (!ok) {
        await new Promise((resolve, reject) => {
          socket.once('drain', resolve);
          socket.once('error', reject);
        });
      }

      const now = Date.now();
      if (now - lastBc > 300 || sent >= file.size) {
        t._bytes = sent;
        t.progress = Math.min(100, Math.round(sent / file.size * 100));
        const elapsed = (now - t._start) / 1000;
        t.speed = elapsed > 0 ? sent / elapsed : 0;
        broadcast();
        lastBc = now;
      }
    }

    socket.end();
    t.status = 'completed';
    t.progress = 100;
    broadcast();
    console.log(`  Sent: ${file.name} → ${peer.device_name}`);
  } catch (e) {
    t.status = 'failed';
    t.error = e.message;
    broadcast();
    console.error(`  Send error: ${e.message}`);
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Utility
// ──────────────────────────────────────────────────────────────────────────────
function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
  return (bytes / 1073741824).toFixed(1) + ' GB';
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP-based discovery (no firewall rules needed — outbound TCP only)
// ──────────────────────────────────────────────────────────────────────────────
function getSubnetIPs() {
  const results = [];
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const iface of ifaces) {
      if (iface.family === 'IPv4' && !iface.internal && iface.netmask) {
        const ipParts = iface.address.split('.').map(Number);
        const maskParts = iface.netmask.split('.').map(Number);
        // Only scan /24 or smaller subnets to keep it fast
        if (maskParts[2] === 255) {
          const base = ipParts.slice(0, 3).join('.');
          for (let i = 1; i < 255; i++) {
            const ip = `${base}.${i}`;
            if (ip !== iface.address) results.push(ip);
          }
        }
      }
    }
  }
  return results;
}

function pingHost(ip) {
  return new Promise(resolve => {
    const req = http.get(`http://${ip}:${HTTP_PORT}/api/ping`, { timeout: 800 }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          const d = JSON.parse(body);
          if (d.app === 'lantransfer' && d.device_name !== deviceName) {
            resolve({ ip, device_name: d.device_name, tcp_port: d.tcp_port });
          } else resolve(null);
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
  });
}

let httpScanning = false;
async function httpDiscoveryScan() {
  if (!isDiscoverable || httpScanning) return;
  httpScanning = true;
  try {
    const ips = getSubnetIPs();
    // Scan in batches of 50 to avoid fd exhaustion
    for (let i = 0; i < ips.length; i += 50) {
      const batch = ips.slice(i, i + 50);
      const results = await Promise.all(batch.map(pingHost));
      for (const r of results) {
        if (!r) continue;
        const id = `${r.ip}:${r.tcp_port}`;
        const isNew = !peers.has(id);
        peers.set(id, {
          device_name: r.device_name,
          ip: r.ip,
          tcp_port: r.tcp_port,
          last_seen: Date.now(),
        });
        if (isNew) {
          console.log(`  Found: ${r.device_name} (${r.ip})`);
          broadcast();
        }
      }
    }
  } catch { /* scan error, ignore */ }
  httpScanning = false;
}

// Run HTTP discovery scan every 5 seconds
setInterval(httpDiscoveryScan, 5_000);
// Also run immediately on start (after a short delay for server to be ready)
setTimeout(httpDiscoveryScan, 1_000);

// ──────────────────────────────────────────────────────────────────────────────
// Start
// ──────────────────────────────────────────────────────────────────────────────
server.on('error', err => {
  console.error(`  HTTP error: ${err.message}`);
  if (err.code === 'EADDRINUSE') {
    console.error(`  Port ${HTTP_PORT} is in use. Close the other app or change HTTP_PORT.`);
    process.exit(1);
  }
});

server.listen(HTTP_PORT, () => {
  const ip = getLocalIP();
  console.log('');
  console.log('  ⇄  LAN File Transfer');
  console.log(`  Device:    ${deviceName}`);
  console.log(`  Local IP:  ${ip}`);
  console.log(`  Discovery  HTTP scan (no firewall needed)`);
  console.log(`  Discovery  UDP :${DISCOVERY_PORT} (fallback)`);
  console.log(`  Transfer   TCP :${TRANSFER_PORT}`);
  console.log(`  UI:        http://localhost:${HTTP_PORT}`);
  console.log('');

  // Auto-open browser
  const url = `http://localhost:${HTTP_PORT}`;
  if (process.platform === 'win32') exec(`start "" "${url}"`);
  else if (process.platform === 'darwin') exec(`open "${url}"`);
  else exec(`xdg-open "${url}"`);
});
