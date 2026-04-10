# lantransfer

Peer-to-peer file sharing over your local network. Zero dependencies. Works on Windows, macOS, and Linux.

```
npm install -g @balaji003/lantransfer
lantransfer
```

A browser window opens automatically — select a file, pick a nearby device, and send.

## Features

- **Zero dependencies** — uses only Node.js built-in modules
- **Auto-discovery** — finds devices on your LAN automatically via HTTP subnet scanning (no firewall rules needed) + UDP broadcast fallback
- **Browser UI** — clean dark-themed interface, no extra apps to install
- **Cross-platform** — Windows, macOS, Linux
- **Accept/Reject prompt** — incoming files must be approved before transfer
- **Progress tracking** — real-time speed and progress for all transfers
- **XOR encrypted** — file data is obfuscated during transfer
- **Native file picker** — uses your OS file dialog (PowerShell on Windows, osascript on macOS, zenity/kdialog on Linux)

## Usage

### Install globally and run

```bash
npm install -g @balaji003/lantransfer
lantransfer
```

### Or run directly with npx

```bash
npx @balaji003/lantransfer
```

### Or clone and run

```bash
git clone https://github.com/user/lantransfer.git
cd lantransfer
node server.js
```

## How it works

1. Run `lantransfer` on two (or more) devices connected to the same WiFi/LAN
2. Each device scans the local subnet to find other instances — no firewall configuration needed
3. Devices appear in the sidebar under "Nearby Devices"
4. Click **Browse** to select a file, select a device, and click **Send**
5. The receiver sees a popup to **Accept** or **Reject**
6. Accepted files are saved to the `Downloads` folder

## Ports

| Port  | Protocol | Purpose              |
|-------|----------|----------------------|
| 3000  | HTTP     | Web UI + discovery   |
| 34254 | UDP      | Broadcast discovery (fallback) |
| 34255 | TCP      | File transfer        |

## Requirements

- Node.js >= 16.0.0
- Both devices on the same local network

## License

MIT
