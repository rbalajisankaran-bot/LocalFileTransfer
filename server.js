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
// Application state
// ──────────────────────────────────────────────────────────────────────────────
let isDiscoverable = true;
const deviceName = os.hostname();
let selectedFile = null;              // { path, name, size }
const peers = new Map();              // id → { device_name, ip, tcp_port, last_seen }
const transfers = [];                 // [ TransferEntry ]
const pendingRequests = new Map();    // id → { filename, size, senderName, resolve }
let idCounter = 0;

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
    localIP: getLocalIP(),
    peers: [...peers.entries()].map(([id, p]) => ({
      id, name: p.device_name, ip: p.ip,
    })),
    selectedFile: selectedFile
      ? { name: selectedFile.name, size: selectedFile.size }
      : null,
    transfers: transfers.map(t => ({
      id: t.id, filename: t.filename, size: t.size,
      direction: t.direction, peerName: t.peerName,
      status: t.status, progress: t.progress,
      speed: t.speed, error: t.error,
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
    }
    else if (req.url === '/api/browse') {
      const fp = await openFileDialog();
      if (fp) {
        try {
          const s = fs.statSync(fp);
          selectedFile = { path: fp, name: path.basename(fp), size: s.size };
        } catch { selectedFile = null; }
      }
    }
    else if (req.url === '/api/send') {
      const peer = peers.get(data.peerId);
      if (peer && selectedFile) {
        sendFile(peer, { ...selectedFile });
      }
    }
    else if (req.url === '/api/respond') {
      const pending = pendingRequests.get(data.requestId);
      if (pending) {
        pending.resolve(!!data.accepted);
        pendingRequests.delete(data.requestId);
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

// ──────────────────────────────────────────────────────────────────────────────
// UDP discovery
// ──────────────────────────────────────────────────────────────────────────────
const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true });

udp.on('listening', () => {
  udp.setBroadcast(true);
  console.log(`  Discovery  UDP :${DISCOVERY_PORT}`);
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

tcpServer.listen(TRANSFER_PORT, () => {
  console.log(`  Transfer   TCP :${TRANSFER_PORT}`);
});

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

  // Unique save path in Downloads
  const dl = path.join(os.homedir(), 'Downloads');
  if (!fs.existsSync(dl)) fs.mkdirSync(dl, { recursive: true });
  let savePath = path.join(dl, filename);
  let counter = 1;
  const ext = path.extname(filename);
  const base = path.basename(filename, ext);
  while (fs.existsSync(savePath)) {
    savePath = path.join(dl, `${base} (${counter++})${ext}`);
  }

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
// Windows Firewall check
// ──────────────────────────────────────────────────────────────────────────────
function checkWindowsFirewall() {
  if (process.platform !== 'win32') return;

  const ruleName = 'LAN Transfer (lantransfer)';
  const nodeExe = process.execPath;

  // Check if a firewall rule already exists
  exec(`netsh advfirewall firewall show rule name="${ruleName}"`, { windowsHide: true }, (err, stdout) => {
    if (!err && stdout.includes(ruleName)) {
      if (stdout.includes('Enabled:') && stdout.includes('Yes')) {
        console.log('  Firewall:  Allowed');
        return;
      }
    }

    // No rule — prompt user via UAC elevation
    console.log('');
    console.log('  !! Firewall rule not found for LAN Transfer.');
    console.log('  !! Device discovery requires UDP/TCP through Windows Firewall.');
    console.log('  !! Requesting permission (UAC prompt)...');
    console.log('');

    const addCmd = [
      `netsh advfirewall firewall add rule name="${ruleName}" dir=in action=allow protocol=UDP localport=${DISCOVERY_PORT} program="${nodeExe}" enable=yes`,
      `netsh advfirewall firewall add rule name="${ruleName}" dir=in action=allow protocol=TCP localport=${TRANSFER_PORT} program="${nodeExe}" enable=yes`,
    ].join(' & ');

    const psCmd = `powershell -Command "Start-Process cmd -ArgumentList '/c ${addCmd.replace(/"/g, '\\"')}' -Verb RunAs -Wait"`;

    exec(psCmd, { windowsHide: false, timeout: 60_000 }, (err2) => {
      if (err2) {
        console.log('  !! Firewall permission denied. Discovery will NOT work.');
        console.log('  !! To fix manually:');
        console.log('  !!   1. Open Windows Security > Firewall & network protection');
        console.log('  !!   2. Click "Allow an app through firewall"');
        console.log('  !!   3. Add Node.js and allow Private + Public networks');
        console.log('');
      } else {
        console.log('  Firewall:  Rule added! Discovery should work now.');
      }
    });
  });
}

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
  console.log(`  UI:        http://localhost:${HTTP_PORT}`);
  console.log('');

  // Check firewall on Windows
  checkWindowsFirewall();

  // Auto-open browser
  const url = `http://localhost:${HTTP_PORT}`;
  if (process.platform === 'win32') exec(`start "" "${url}"`);
  else if (process.platform === 'darwin') exec(`open "${url}"`);
  else exec(`xdg-open "${url}"`);
});
