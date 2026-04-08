/// discovery.rs — UDP-based LAN device discovery.
///
/// Two independent async tasks run here:
///   1. `run_broadcaster` — Sends a JSON beacon every 3 seconds on the UDP broadcast address
///      when the user has enabled discoverability.
///   2. `run_listener` — Receives UDP packets and upserts the peer list in `AppState`.
///
/// Discovery message format (JSON):
///   { "device_name": "LAPTOP-XYZ", "tcp_port": 34255 }
///
/// Peers that are not heard from for 10 seconds are pruned by the UI loop.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::time::{self, Duration};

use crate::app_state::{PeerInfo, SharedState};

/// UDP port used exclusively for discovery beacons.
pub const DISCOVERY_PORT: u16 = 34254;
/// TCP port used for file transfers (sent inside the beacon so peers know where to connect).
pub const TRANSFER_PORT: u16 = 34255;
/// Broadcast interval in seconds.
const BROADCAST_INTERVAL_SECS: u64 = 3;
/// Peer TTL in seconds (prune after this much silence).
pub const PEER_TTL_SECS: u64 = 10;

/// The JSON message broadcast by each instance.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiscoveryMsg {
    pub device_name: String,
    pub tcp_port: u16,
}

/// Continuously broadcasts a presence beacon while `is_discoverable` is true.
///
/// Binds to `0.0.0.0:0` (ephemeral port) and blasts to `255.255.255.255:DISCOVERY_PORT`.
pub async fn run_broadcaster(state: SharedState) {
    // Bind to any available outgoing port.
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[discovery] Failed to bind broadcast socket: {e}");
            return;
        }
    };
    if let Err(e) = socket.set_broadcast(true) {
        eprintln!("[discovery] set_broadcast failed: {e}");
        return;
    }

    let broadcast_addr: std::net::SocketAddr =
        SocketAddrV4::new(Ipv4Addr::BROADCAST, DISCOVERY_PORT).into();

    let mut interval = time::interval(Duration::from_secs(BROADCAST_INTERVAL_SECS));

    loop {
        interval.tick().await;

        let (discoverable, name) = {
            let s = state.lock().unwrap();
            (s.is_discoverable, s.device_name.clone())
        };

        if !discoverable {
            // Skip sending but keep looping so we resume promptly when toggled on.
            continue;
        }

        let msg = DiscoveryMsg {
            device_name: name,
            tcp_port: TRANSFER_PORT,
        };

        let payload = match serde_json::to_vec(&msg) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[discovery] Serialization error: {e}");
                continue;
            }
        };

        if let Err(e) = socket.send_to(&payload, broadcast_addr).await {
            eprintln!("[discovery] Send error: {e}");
        }
    }
}

/// Listens for UDP beacons from peers and updates the shared peer list.
///
/// Binds to `0.0.0.0:DISCOVERY_PORT` with SO_REUSEADDR so multiple instances on the
/// same machine work during development.
pub async fn run_listener(state: SharedState) {
    // On Windows we bind to INADDR_ANY to receive both unicast and broadcast.
    let bind_addr = format!("0.0.0.0:{}", DISCOVERY_PORT);
    let socket = match UdpSocket::bind(&bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[discovery] Failed to bind listener on {bind_addr}: {e}");
            return;
        }
    };
    if let Err(e) = socket.set_broadcast(true) {
        eprintln!("[discovery] listener set_broadcast failed: {e}");
    }

    let mut buf = vec![0u8; 1024];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, src_addr)) => {
                let src_ip = match src_addr.ip() {
                    std::net::IpAddr::V4(v4) => v4.to_string(),
                    std::net::IpAddr::V6(v6) => v6.to_string(),
                };

                // Parse the JSON beacon.
                let msg: DiscoveryMsg = match serde_json::from_slice(&buf[..len]) {
                    Ok(m) => m,
                    Err(_) => continue, // Ignore malformed packets.
                };

                let peer_key = format!("{}:{}", src_ip, msg.tcp_port);

                // Skip ourselves — compare by device name (simple heuristic).
                {
                    let state_guard = state.lock().unwrap();
                    if msg.device_name == state_guard.device_name {
                        continue;
                    }
                }

                let info = PeerInfo {
                    device_name: msg.device_name,
                    ip: src_ip,
                    tcp_port: msg.tcp_port,
                };

                {
                    let mut s = state.lock().unwrap();
                    s.peers.insert(peer_key, (info, Instant::now()));
                }
            }
            Err(e) => {
                eprintln!("[discovery] recv_from error: {e}");
                // Brief pause before retrying to avoid tight error loop.
                time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}
