# LAN File Transfer App — Implementation Plan

A single-binary Windows `.exe` built with Rust that lets users on the same WiFi network discover each other and send/receive files with a live GUI.

## Proposed Changes

### Project Root — `e:\LocalFileShare\lan-transfer\`

---

#### [NEW] `Cargo.toml`
| Crate | Purpose |
|---|---|
| `eframe` / `egui` | Immediate-mode GUI |
| `tokio` | Async runtime (UDP + TCP) |
| `serde` / `serde_json` | Message serialization |
| `rfd` | Native file picker dialog |
| `hostname` | Get local machine hostname |
| `dirs` | Locate Downloads folder |
| `chrono` | Timestamps |

`lto = true`, `opt-level = "z"`, `strip = true` for minimal binary.

---

#### [NEW] `src/main.rs`
- Launches `tokio` runtime
- Starts discovery broadcaster + listener tasks
- Starts TCP listener task
- Starts `eframe` event loop with shared `AppState`

---

#### [NEW] `src/app_state.rs`
Shared state (`Arc<Mutex<AppState>>`) holding:
- `peers: HashMap<String, (PeerInfo, Instant)>` — discovered devices with TTL
- `transfers: Vec<TransferStatus>` — active/queued file transfers
- `incoming_requests: Vec<IncomingRequest>` — Accept / Reject prompts
- `is_discoverable: bool` — toggle
- `device_name: String` — local hostname

---

#### [NEW] `src/discovery.rs`
- `broadcast_presence(state, socket)` — UDP broadcast every 3 s when discoverable; sends `DiscoveryMsg { device_name, ip, port }` as JSON
- `listen_for_peers(state, socket)` — receives UDP packets, upserts peer list
- Peers expire after 10 s of silence (checked in UI repaint loop)

---

#### [NEW] `src/transfer.rs`
- `send_file(peer_addr, file_path, tx_progress)` — opens TCP, sends header JSON `{ filename, size }`, then streams bytes; reports progress via `mpsc` channel; applies XOR cipher with fixed key
- `run_receiver(state, listener)` — accepts incoming TCP; reads header, pushes `IncomingRequest` to state; if accepted, writes to `Downloads/` with XOR decrypt; reports progress

---

#### [NEW] `src/ui.rs`
- `render(ctx, state, ...)` — called every frame by eframe
- Toggle for **Make Discoverable**
- Peer list panel (click to select)
- **Browse File** button (via `rfd::FileDialog`)
- **Send** button → spawns `send_file` task
- Progress bar + speed indicator
- Modal overlay for incoming Accept/Reject prompt

---

## Verification Plan

### Automated Tests
None (GUI/networking app is hard to automate end-to-end). Build correctness is the primary check.

### Build Verification
```
cd e:\LocalFileShare\lan-transfer
cargo build --release
```
Expected: zero errors, `target\release\lan-transfer.exe` produced.

### Manual Verification
1. Run two instances (`lan-transfer.exe`) on the same machine (or two laptops on same WiFi).
2. Toggle **Make Discoverable** on both — each should appear in the other's device list within ~5 s.
3. On instance A, click a peer from the list, click **Browse File**, select any file, click **Send**.
4. Instance B shows an **Accept / Reject** dialog.
5. Accept → progress bar fills on both ends → file appears in `%USERPROFILE%\Downloads\`.
6. Toggle **Make Discoverable** off on one instance → it should disappear from the other's list within ~10 s.
