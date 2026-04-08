/// transfer.rs — TCP-based file send and receive.
///
/// Protocol (over a single TCP stream):
///   1. Sender writes a length-prefixed JSON header: 8-byte LE u64 length + JSON bytes.
///      Header: { "filename": "...", "size": 12345 }
///   2. If the receiver accepts, it writes a single byte `1u8` back; reject = `0u8`.
///   3. Sender streams the file bytes XOR-encrypted with XOR_KEY.
///   4. When all bytes are sent the TCP connection is closed.
///
/// Progress is reported via an `UnboundedSender<u64>` that delivers cumulative bytes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::app_state::{
    IncomingRequest, SharedState, TransferDirection, TransferEntry, TransferStatus,
};
use crate::discovery::TRANSFER_PORT;

/// XOR encryption key — simple obfuscation as requested.
/// Every byte in the file stream is XORed with the repeating pattern of this key.
const XOR_KEY: &[u8] = b"LAN-XFER-KEY-2024";

/// Apply (or un-apply) the XOR cipher in-place on a byte slice.
#[inline]
fn xor_cipher(data: &mut [u8], offset: usize) {
    let key_len = XOR_KEY.len();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte ^= XOR_KEY[(offset + i) % key_len];
    }
}

/// JSON header sent at the start of every transfer.
#[derive(Serialize, Deserialize, Debug)]
pub struct TransferHeader {
    /// Original file name (no path).
    pub filename: String,
    /// Total file size in bytes.
    pub size: u64,
    /// Sender's display name.
    pub sender_name: String,
}

/// Send a file to a remote peer.
///
/// Writes progress back via `progress_tx` (cumulative bytes transferred).
/// Returns `Ok(true)` if the peer accepted, `Ok(false)` if rejected, `Err` on I/O failure.
pub async fn send_file(
    peer_ip: String,
    peer_port: u16,
    file_path: PathBuf,
    sender_name: String,
    progress_tx: mpsc::UnboundedSender<u64>,
) -> anyhow::Result<bool> {
    let addr = format!("{}:{}", peer_ip, peer_port);
    let mut stream = TcpStream::connect(&addr).await?;

    // --- Build and send header ---
    let filename = file_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let metadata = tokio::fs::metadata(&file_path).await?;
    let total_size = metadata.len();

    let header = TransferHeader {
        filename,
        size: total_size,
        sender_name,
    };
    let header_bytes = serde_json::to_vec(&header)?;
    let header_len = header_bytes.len() as u64;

    stream.write_u64_le(header_len).await?;
    stream.write_all(&header_bytes).await?;

    // --- Wait for accept/reject byte ---
    let decision = stream.read_u8().await?;
    if decision == 0 {
        return Ok(false); // Rejected.
    }

    // --- Stream file with XOR cipher ---
    let mut file = File::open(&file_path).await?;
    let mut buf = vec![0u8; 64 * 1024]; // 64 KiB chunks.
    let mut sent: u64 = 0;

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        xor_cipher(&mut buf[..n], sent as usize);
        stream.write_all(&buf[..n]).await?;
        sent += n as u64;
        let _ = progress_tx.send(sent);
    }

    stream.flush().await?;
    Ok(true)
}

/// Run the TCP listener that accepts incoming file transfers.
///
/// Spawns a new task per accepted connection so the listener is never blocked.
pub async fn run_receiver(state: SharedState) {
    let bind_addr = format!("0.0.0.0:{}", TRANSFER_PORT);
    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[transfer] Failed to bind TCP listener on {bind_addr}: {e}");
            return;
        }
    };

    println!("[transfer] Listening on {bind_addr}");

    // Counter to generate unique IDs for pending requests.
    let id_counter = Arc::new(Mutex::new(0u64));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state_clone = Arc::clone(&state);
                let id_counter_clone = Arc::clone(&id_counter);
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_incoming(stream, state_clone, id_counter_clone).await
                    {
                        eprintln!("[transfer] Incoming error: {e}");
                    }
                });
            }
            Err(e) => {
                eprintln!("[transfer] accept error: {e}");
            }
        }
    }
}

/// Handle a single incoming TCP connection end-to-end.
async fn handle_incoming(
    mut stream: TcpStream,
    state: SharedState,
    id_counter: Arc<Mutex<u64>>,
) -> anyhow::Result<()> {
    // --- Read header ---
    let header_len = stream.read_u64_le().await? as usize;
    let mut header_buf = vec![0u8; header_len];
    stream.read_exact(&mut header_buf).await?;
    let header: TransferHeader = serde_json::from_slice(&header_buf)?;

    // --- Generate a unique request ID ---
    let req_id = {
        let mut counter = id_counter.lock().await;
        *counter += 1;
        *counter
    };

    // --- Create a oneshot channel for the UI to return the decision ---
    let (decision_tx, decision_rx) = oneshot::channel::<bool>();

    {
        let mut s = state.lock().unwrap();
        s.incoming_requests.push(IncomingRequest {
            id: req_id,
            peer_name: header.sender_name.clone(),
            filename: header.filename.clone(),
            size_bytes: header.size,
            decision_tx: Arc::new(decision_tx),
        });
    }

    // --- Wait for user decision (with 60 s timeout) ---
    let accepted = tokio::time::timeout(
        tokio::time::Duration::from_secs(60),
        decision_rx,
    )
    .await
    .unwrap_or(Ok(false))
    .unwrap_or(false);

    // Send the decision byte back to the sender.
    stream.write_u8(if accepted { 1 } else { 0 }).await?;
    stream.flush().await?;

    if !accepted {
        return Ok(());
    }

    // --- Prepare destination path in Downloads ---
    let downloads_dir = dirs::download_dir().unwrap_or_else(|| PathBuf::from("."));
    let dest_path = unique_path(&downloads_dir, &header.filename);

    // --- Create a transfer entry for UI progress ---
    let entry_idx = {
        let mut s = state.lock().unwrap();
        let idx = s.transfers.len();
        s.transfers.push(TransferEntry {
            filename: header.filename.clone(),
            total_bytes: header.size,
            transferred_bytes: 0,
            direction: TransferDirection::Receive,
            status: TransferStatus::InProgress,
            peer_name: header.sender_name.clone(),
            speed_bps: 0.0,
            started_at: Instant::now(),
            last_sample_bytes: 0,
            last_sample_time: Instant::now(),
        });
        idx
    };

    // --- Receive file bytes ---
    let mut dest_file = File::create(&dest_path).await?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut received: u64 = 0;

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let mut chunk = buf[..n].to_vec();
        xor_cipher(&mut chunk, received as usize);
        dest_file.write_all(&chunk).await?;
        received += n as u64;

        // Update shared state for UI progress.
        let mut s = state.lock().unwrap();
        if let Some(entry) = s.transfers.get_mut(entry_idx) {
            entry.transferred_bytes = received;
            // Speed update every 256 KiB.
            let elapsed = entry.last_sample_time.elapsed().as_secs_f64();
            if elapsed >= 0.5 {
                let delta_bytes = received - entry.last_sample_bytes;
                entry.speed_bps = delta_bytes as f64 / elapsed;
                entry.last_sample_bytes = received;
                entry.last_sample_time = Instant::now();
            }
        }

        if received >= header.size {
            break;
        }
    }

    dest_file.flush().await?;

    // Mark complete.
    {
        let mut s = state.lock().unwrap();
        if let Some(entry) = s.transfers.get_mut(entry_idx) {
            entry.transferred_bytes = header.size;
            entry.status = TransferStatus::Completed;
        }
    }

    println!("[transfer] Saved file to {}", dest_path.display());
    Ok(())
}

/// Return a destination path that doesn't collide with existing files.
/// If `downloads/file.txt` exists, tries `downloads/file (1).txt`, etc.
fn unique_path(dir: &Path, filename: &str) -> PathBuf {
    let base = Path::new(filename);
    let stem = base
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let ext = base
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();

    let mut candidate = dir.join(filename);
    let mut counter = 1u32;
    while candidate.exists() {
        candidate = dir.join(format!("{} ({}){}", stem, counter, ext));
        counter += 1;
    }
    candidate
}

/// Task that listens on a channel for outgoing send requests from the UI.
///
/// The UI sends `(peer_ip, peer_tcp_port, file_path)` tuples.
/// This function handles them sequentially (queued).
pub async fn run_sender_queue(
    state: SharedState,
    mut rx: mpsc::UnboundedReceiver<(String, u16, PathBuf)>,
) {
    while let Some((peer_ip, peer_port, file_path)) = rx.recv().await {
        let sender_name = {
            state.lock().unwrap().device_name.clone()
        };

        let filename = file_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let total_bytes = if let Ok(meta) = std::fs::metadata(&file_path) {
            meta.len()
        } else {
            0
        };

        // Create a transfer entry.
        let entry_idx = {
            let mut s = state.lock().unwrap();
            let idx = s.transfers.len();
            s.transfers.push(TransferEntry {
                filename: filename.clone(),
                total_bytes,
                transferred_bytes: 0,
                direction: TransferDirection::Send,
                status: TransferStatus::InProgress,
                peer_name: peer_ip.clone(),
                speed_bps: 0.0,
                started_at: Instant::now(),
                last_sample_bytes: 0,
                last_sample_time: Instant::now(),
            });
            idx
        };

        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<u64>();

        // Spawn progress forwarder — runs concurrently with the send.
        let state_clone = Arc::clone(&state);
        tokio::spawn(async move {
            while let Some(bytes) = progress_rx.recv().await {
                let mut s = state_clone.lock().unwrap();
                if let Some(entry) = s.transfers.get_mut(entry_idx) {
                    entry.transferred_bytes = bytes;
                    let elapsed = entry.last_sample_time.elapsed().as_secs_f64();
                    if elapsed >= 0.5 {
                        let delta = bytes - entry.last_sample_bytes;
                        entry.speed_bps = delta as f64 / elapsed;
                        entry.last_sample_bytes = bytes;
                        entry.last_sample_time = Instant::now();
                    }
                }
            }
        });

        // Update peer_name to something readable (look it up in state).
        {
            let mut s = state.lock().unwrap();
            let peer_key = format!("{}:{}", peer_ip, peer_port);
            if let Some((peer_info, _)) = s.peers.get(&peer_key).cloned() {
                if let Some(entry) = s.transfers.get_mut(entry_idx) {
                    entry.peer_name = peer_info.device_name.clone();
                }
            }
        }

        match send_file(
            peer_ip.clone(),
            peer_port,
            file_path,
            sender_name,
            progress_tx,
        )
        .await
        {
            Ok(true) => {
                let mut s = state.lock().unwrap();
                if let Some(entry) = s.transfers.get_mut(entry_idx) {
                    entry.transferred_bytes = total_bytes;
                    entry.status = TransferStatus::Completed;
                }
            }
            Ok(false) => {
                let mut s = state.lock().unwrap();
                if let Some(entry) = s.transfers.get_mut(entry_idx) {
                    entry.status = TransferStatus::Failed("Rejected by peer".to_string());
                }
            }
            Err(e) => {
                let mut s = state.lock().unwrap();
                if let Some(entry) = s.transfers.get_mut(entry_idx) {
                    entry.status = TransferStatus::Failed(e.to_string());
                }
            }
        }
    }
}
