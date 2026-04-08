/// app_state.rs — Shared state between the async networking tasks and the egui UI thread.
///
/// All networking tasks and the UI hold an `Arc<Mutex<AppState>>` clone.
/// The UI reads / renders on every frame; networking tasks write when events arrive.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Information about a discovered peer on the LAN.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Human-readable device name (hostname).
    pub device_name: String,
    /// IP address string, e.g. "192.168.1.42".
    pub ip: String,
    /// TCP port the peer listens on for file transfers.
    pub tcp_port: u16,
}

/// Represents a live or completed file transfer (send or receive).
#[derive(Debug, Clone)]
pub struct TransferEntry {
    /// Name of the file being transferred.
    pub filename: String,
    /// Total size in bytes.
    pub total_bytes: u64,
    /// Bytes transferred so far.
    pub transferred_bytes: u64,
    /// Transfer direction.
    pub direction: TransferDirection,
    /// Current status.
    pub status: TransferStatus,
    /// Peer name for display.
    pub peer_name: String,
    /// Computed transfer speed in bytes/s (updated periodically).
    pub speed_bps: f64,
    /// When the transfer started (for speed calculation).
    pub started_at: Instant,
    /// Bytes transferred at last speed sample.
    pub last_sample_bytes: u64,
    /// Instant of last speed sample.
    pub last_sample_time: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransferDirection {
    Send,
    Receive,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransferStatus {
    InProgress,
    Completed,
    Failed(String),
    WaitingAccept,
}

impl TransferEntry {
    pub fn progress(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.transferred_bytes as f32 / self.total_bytes as f32).min(1.0)
    }
}

/// A pending incoming file request waiting for user Accept/Reject.
#[derive(Debug, Clone)]
pub struct IncomingRequest {
    /// Unique ID for correlation.
    pub id: u64,
    /// Who is sending the file.
    pub peer_name: String,
    /// File name.
    pub filename: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Channel to send the user's decision back to the receiver task.
    pub decision_tx: Arc<tokio::sync::oneshot::Sender<bool>>,
}

/// Shared state of the entire application.
pub struct AppState {
    /// Whether this device is advertising itself on the LAN.
    pub is_discoverable: bool,

    /// Local device name shown in the UI and broadcast.
    pub device_name: String,

    /// Discovered peers: key is `"ip:port"`, value is (info, last_seen).
    pub peers: HashMap<String, (PeerInfo, Instant)>,

    /// Currently selected peer (for sending).
    pub selected_peer: Option<String>,

    /// File chosen by the user to send.
    pub selected_file: Option<PathBuf>,

    /// Active and completed transfers.
    pub transfers: Vec<TransferEntry>,

    /// Pending Accept/Reject prompts from incoming connections.
    pub incoming_requests: Vec<IncomingRequest>,

    /// Channel for the UI to kick off a send operation.
    /// Sent as (peer_ip, peer_tcp_port, file_path).
    pub send_tx: Option<tokio::sync::mpsc::UnboundedSender<(String, u16, PathBuf)>>,
}

impl AppState {
    pub fn new(device_name: String) -> Self {
        Self {
            is_discoverable: true,
            device_name,
            peers: HashMap::new(),
            selected_peer: None,
            selected_file: None,
            transfers: Vec::new(),
            incoming_requests: Vec::new(),
            send_tx: None,
        }
    }

    /// Remove peers that haven't been seen for more than `timeout_secs` seconds.
    pub fn prune_stale_peers(&mut self, timeout_secs: u64) {
        let now = Instant::now();
        self.peers.retain(|_, (_, last_seen)| {
            now.duration_since(*last_seen).as_secs() < timeout_secs
        });
    }
}

pub type SharedState = Arc<Mutex<AppState>>;
