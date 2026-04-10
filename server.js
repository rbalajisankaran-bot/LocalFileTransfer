#!/usr/bin/env node

/**
 * server.js — LAN File Transfer (Node.js rewrite)
 *
 * Zero external dependencies. Uses built-in modules only.
 *   - HTTP + UDP discovery
 *   - TCP file transfer on port 34255
 *   - E2E encryption: ECDH key exchange + AES-256-CTR
 *
 * Run:  node server.js
 * Then open http://localhost:3000 in a browser.
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
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
const PEER_TIMEOUT = 10_000;   // ms
const BROADCAST_INTERVAL = 3_000; // ms
const ECDH_CURVE = 'prime256v1'; // NIST P-256
const PKG = JSON.parse(fs.readFileSync(path.join(__dirname, 'package.json'), 'utf-8'));
const CURRENT_VERSION = PKG.version;
const PKG_NAME = PKG.name;
let latestVersion = null; // filled by update check

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
let pendingRequest = null;            // { filename, size, senderName, resolve } or null
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
    currentVersion: CURRENT_VERSION,
    latestVersion,
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
    incomingRequest: pendingRequest
      ? { filename: pendingRequest.filename, size: pendingRequest.size, senderName: pendingRequest.senderName }
      : null,
  });
}

function broadcast() {
  const msg = `data: ${serializeState()}\n\n`;
  for (const res of sseClients) {
    try { res.write(msg); } catch { /* client gone */ }
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers — E2E encryption (ECDH + AES-256-CTR)
// ──────────────────────────────────────────────────────────────────────────────
function createKeyPair() {
  const ecdh = crypto.createECDH(ECDH_CURVE);
  ecdh.generateKeys();
  return ecdh;
}

function deriveKey(ecdh, peerPubKey) {
  const shared = ecdh.computeSecret(peerPubKey);
  // SHA-256 the shared secret to get a 32-byte AES key
  return crypto.createHash('sha256').update(shared).digest();
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
  console.log(`  [http] ${req.method} ${req.url}`);

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
    console.log(`  [sse] Client connected (total: ${sseClients.size})`);
    req.on('close', () => {
      sseClients.delete(res);
      console.log(`  [sse] Client disconnected (total: ${sseClients.size})`);
    });
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
      // Respond immediately so we don't block the HTTP connection
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      if (browseInProgress) {
        console.log('  [browse] Dialog already open, ignoring');
      } else {
        browseInProgress = true;
        console.log('  [browse] Opening file dialog...');
        try {
          const fp = await openFileDialog();
          browseInProgress = false;
          if (fp) {
            const s = fs.statSync(fp);
            const file = { id: String(idCounter++), path: fp, name: path.basename(fp), size: s.size };
            sharedFiles.push(file);
            console.log(`  [browse] Added: ${file.name} (${formatSize(file.size)})`);
          } else {
            console.log('  [browse] Cancelled by user');
          }
        } catch (e) {
          browseInProgress = false;
          console.error(`  [browse] Error: ${e.message}`);
        }
        broadcast();
      }
      return; // already responded
    }
    else if (req.url === '/api/remove-file') {
      const idx = sharedFiles.findIndex(f => f.id === data.fileId);
      if (idx !== -1) {
        console.log(`  [files] Removed: ${sharedFiles[idx].name}`);
        sharedFiles.splice(idx, 1);
      } else {
        console.log(`  [files] Remove failed — fileId not found: ${data.fileId}`);
      }
    }
    else if (req.url === '/api/send') {
      const peer = peers.get(data.peerId);
      const file = sharedFiles.find(f => f.id === data.fileId);
      if (peer && file) {
        console.log(`  [send] ${file.name} -> ${peer.device_name} (${peer.ip})`);
        sendFile(peer, { ...file }).catch(e => console.error(`  [send] Unhandled: ${e.message}`));
      } else {
        console.log(`  [send] Failed — peer: ${!!peer} (${data.peerId}), file: ${!!file} (${data.fileId})`);
      }
    }
    else if (req.url === '/api/rename') {
      const newName = (data.name || '').trim();
      if (newName && newName.length <= 50) {
        deviceName = newName;
        config.deviceName = newName;
        saveConfig(config);
        console.log(`  [rename] Device name set to: ${deviceName}`);
      } else {
        console.log(`  [rename] Invalid name: "${data.name}"`);
      }
    }
    else if (req.url === '/api/set-download-dir') {
      // Respond immediately so we don't block the HTTP connection
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      console.log('  [config] Opening folder dialog...');
      try {
        const dir = await openFolderDialog();
        if (dir) {
          downloadDir = dir;
          config.downloadDir = dir;
          saveConfig(config);
          console.log(`  [config] Download dir set to: ${downloadDir}`);
        } else {
          console.log('  [config] Folder dialog cancelled');
        }
      } catch (e) {
        console.error(`  [config] Folder dialog error: ${e.message}`);
      }
      broadcast();
      return; // already responded
    }
    else if (req.url === '/api/respond') {
      console.log(`  [respond] Received: accepted=${data.accepted}`);
      if (pendingRequest) {
        pendingRequest.resolve(!!data.accepted);
        console.log(`  [respond] ${data.accepted ? 'Accepted' : 'Rejected'}: ${pendingRequest.filename}`);
        pendingRequest = null;
      } else {
        console.log(`  [respond] WARNING: No pending request`);
      }
    }
    else if (req.url === '/api/open-folder') {
      const transferId = data.transferId;
      const t = transfers.find(tr => tr.id === transferId);
      if (t && t.savePath) {
        console.log(`  [open] Opening folder for: ${t.savePath}`);
        if (fs.existsSync(t.savePath)) {
          if (process.platform === 'win32') exec(`explorer /select,"${t.savePath}"`);
          else if (process.platform === 'darwin') exec(`open -R "${t.savePath}"`);
          else exec(`xdg-open "${path.dirname(t.savePath)}"`);
        } else {
          console.log(`  [open] File no longer exists: ${t.savePath}`);
          // Open the directory instead
          const dir = path.dirname(t.savePath);
          if (fs.existsSync(dir)) {
            if (process.platform === 'win32') exec(`explorer "${dir}"`);
            else if (process.platform === 'darwin') exec(`open "${dir}"`);
            else exec(`xdg-open "${dir}"`);
          }
        }
      } else {
        console.log(`  [open] Failed — transferId: ${transferId}, found: ${!!t}, savePath: ${t ? t.savePath : 'N/A'}`);
      }
    }
    else if (req.url === '/api/check-update') {
      checkForUpdate();
    }
    else if (req.url === '/api/do-update') {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      console.log(`  [update] Running: npm install -g ${PKG_NAME}`);
      exec(`npm install -g ${PKG_NAME}`, { timeout: 120_000 }, (err, stdout, stderr) => {
        if (err) {
          console.error(`  [update] Failed: ${err.message}`);
          console.error(stderr);
        } else {
          console.log(`  [update] Success! Restarting...`);
          console.log(stdout);
          // Restart the process
          setTimeout(() => {
            const args = process.argv.slice(1);
            const child = require('child_process').spawn(process.execPath, args, {
              detached: true,
              stdio: 'ignore',
            });
            child.unref();
            process.exit(0);
          }, 500);
        }
      });
      return;
    }
    else if (req.url === '/api/shutdown') {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      console.log('\n  Server stopped by user.');
      setTimeout(() => process.exit(0), 200);
      return;
    }
    else {
      console.log(`  [http] Unknown POST endpoint: ${req.url}`);
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
  const remoteAddr = `${socket.remoteAddress}:${socket.remotePort}`;
  console.log(`  [recv] TCP connection from ${remoteAddr}`);

  // Read header length (8 bytes, little-endian u64)
  const lenBuf = await readExact(socket, 8);
  const headerLen = Number(lenBuf.readBigUInt64LE(0));
  console.log(`  [recv] Header length: ${headerLen} bytes`);

  // Read header JSON
  const headerBuf = await readExact(socket, headerLen);
  const header = JSON.parse(headerBuf.toString('utf-8'));
  const { filename, size, sender_name } = header;

  console.log(`  [recv] Incoming: "${filename}" (${formatSize(size)}) from ${sender_name}`);

  // Prompt user for accept / reject
  console.log(`  [recv] Waiting for user decision...`);

  const accepted = await new Promise(resolve => {
    pendingRequest = { filename, size, senderName: sender_name, resolve };
    broadcast();

    // Auto-reject if sender disconnects while waiting
    socket.once('close', () => {
      if (pendingRequest && pendingRequest.resolve === resolve) {
        console.log(`  [recv] Sender disconnected while waiting, auto-rejecting`);
        pendingRequest = null;
        resolve(false);
        broadcast();
      }
    });
  });

  console.log(`  [recv] User decision: ${accepted ? 'ACCEPTED' : 'REJECTED'}`);

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
  console.log(`  [recv] Saving to: ${savePath}`);

  // ── E2E key exchange: receive sender's public key, send ours ──
  const pubKeyLenBuf = await readExact(socket, 2);
  const pubKeyLen = pubKeyLenBuf.readUInt16BE(0);
  const senderPubKey = await readExact(socket, pubKeyLen);
  console.log(`  [recv] Got sender public key (${pubKeyLen} bytes)`);

  const ecdh = createKeyPair();
  const myPubKey = ecdh.getPublicKey();
  const keyLenBuf = Buffer.alloc(2);
  keyLenBuf.writeUInt16BE(myPubKey.length);
  socket.write(keyLenBuf);
  socket.write(myPubKey);
  console.log(`  [recv] Sent our public key (${myPubKey.length} bytes)`);

  // Derive AES key + receive IV
  const aesKey = deriveKey(ecdh, senderPubKey);
  const iv = await readExact(socket, 16);
  console.log(`  [recv] AES key derived, IV received — decrypting with AES-256-CTR`);

  const decipher = crypto.createDecipheriv('aes-256-ctr', aesKey, iv);
  const ws = fs.createWriteStream(savePath);
  let received = 0;
  let lastBc = 0;

  socket.on('data', chunk => {
    const decrypted = decipher.update(chunk);
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
      const final = decipher.final();
      if (final.length > 0) ws.write(final);
      ws.end();
      t.status = 'completed';
      t.progress = 100;
      broadcast();
      console.log(`  [recv] Complete: ${savePath} (${formatSize(received)})`);
      resolve();
    });
    socket.on('error', err => {
      ws.end();
      t.status = 'failed';
      t.error = err.message;
      broadcast();
      console.error(`  [recv] Socket error: ${err.message}`);
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
    console.log(`  [send] Connected to ${peer.ip}:${peer.tcp_port}`);

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
      console.log(`  [send] Rejected by ${peer.device_name}`);
      return;
    }

    console.log(`  [send] Accepted — starting E2E key exchange`);

    // ── E2E key exchange: send our public key, receive receiver's ──
    const ecdh = createKeyPair();
    const myPubKey = ecdh.getPublicKey();
    const keyLenBuf = Buffer.alloc(2);
    keyLenBuf.writeUInt16BE(myPubKey.length);
    socket.write(keyLenBuf);
    socket.write(myPubKey);
    console.log(`  [send] Sent our public key (${myPubKey.length} bytes)`);

    const recvKeyLenBuf = await readExact(socket, 2);
    const recvKeyLen = recvKeyLenBuf.readUInt16BE(0);
    const recvPubKey = await readExact(socket, recvKeyLen);
    console.log(`  [send] Got receiver public key (${recvKeyLen} bytes)`);

    // Derive AES key, generate IV, send IV
    const aesKey = deriveKey(ecdh, recvPubKey);
    const iv = crypto.randomBytes(16);
    socket.write(iv);
    console.log(`  [send] AES key derived, IV sent — encrypting with AES-256-CTR`);

    t.status = 'in_progress';
    t._start = Date.now();
    broadcast();

    // Stream file with AES-256-CTR encryption
    const cipher = crypto.createCipheriv('aes-256-ctr', aesKey, iv);
    const rs = fs.createReadStream(file.path, { highWaterMark: 65536 });
    let sent = 0;
    let lastBc = 0;

    for await (const chunk of rs) {
      const encrypted = cipher.update(Buffer.from(chunk));
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

    // Write final cipher block
    const final = cipher.final();
    if (final.length > 0) socket.write(final);

    socket.end();
    t.status = 'completed';
    t.progress = 100;
    broadcast();
    console.log(`  [send] Complete: ${file.name} → ${peer.device_name}`);
  } catch (e) {
    t.status = 'failed';
    t.error = e.message;
    broadcast();
    console.error(`  [send] Error: ${e.message}`);
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// Update checker
// ──────────────────────────────────────────────────────────────────────────────
function checkForUpdate() {
  const url = `https://registry.npmjs.org/${PKG_NAME}/latest`;
  console.log(`  [update] Checking ${url}`);
  https.get(url, { timeout: 5000 }, res => {
    let body = '';
    res.on('data', c => body += c);
    res.on('end', () => {
      try {
        const data = JSON.parse(body);
        latestVersion = data.version || null;
        console.log(`  [update] Current: ${CURRENT_VERSION}, Latest: ${latestVersion}`);
        if (latestVersion && latestVersion !== CURRENT_VERSION) {
          console.log(`  [update] Update available! Run: npm install -g ${PKG_NAME}`);
        }
        broadcast();
      } catch (e) {
        console.error(`  [update] Parse error: ${e.message}`);
      }
    });
  }).on('error', e => {
    console.error(`  [update] Check failed: ${e.message}`);
  });
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
  console.log(`  Encryption ECDH + AES-256-CTR (E2E)`);
  console.log(`  Version    ${CURRENT_VERSION}`);
  console.log(`  UI:        http://localhost:${HTTP_PORT}`);
  console.log('');

  // Check for updates on startup
  checkForUpdate();

  // Auto-open browser
  const url = `http://localhost:${HTTP_PORT}`;
  if (process.platform === 'win32') exec(`start "" "${url}"`);
  else if (process.platform === 'darwin') exec(`open "${url}"`);
  else exec(`xdg-open "${url}"`);
});
