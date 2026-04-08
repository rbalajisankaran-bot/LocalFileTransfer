#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console on release

/// main.rs — Entry point for the LAN File Transfer application.
///
/// Boot sequence:
///   1. Resolve local hostname → build `AppState`.
///   2. Spin up a multi-threaded Tokio runtime.
///   3. Spawn async tasks: UDP broadcaster, UDP listener, TCP receiver, send-queue worker.
///   4. Hand control to `eframe` for the GUI event loop.

mod app_state;
mod discovery;
mod transfer;
mod ui;

use std::sync::{Arc, Mutex};

use app_state::AppState;
use ui::LanTransferApp;

fn main() -> eframe::Result<()> {
    // ── Resolve hostname ──────────────────────────────────────────────────────
    let device_name = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "Unknown-Device".to_string());

    // ── Build shared state ────────────────────────────────────────────────────
    let state = Arc::new(Mutex::new(AppState::new(device_name)));

    // ── Tokio runtime (multi-threaded) ────────────────────────────────────────
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime"),
    );

    // ── Async tasks ───────────────────────────────────────────────────────────
    {
        // Enter the runtime so that tokio primitives (channels, etc.) can be
        // created safely outside of an async block.
        let _enter = rt.enter();

        // Sender channel: created here so we can store the Sender in AppState
        // before spawning the worker task.
        let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut s = state.lock().unwrap();
            s.send_tx = Some(send_tx);
        }

        // UDP broadcaster — sends presence beacons every 3 s.
        rt.spawn(discovery::run_broadcaster(Arc::clone(&state)));

        // UDP listener — receives beacons from peers.
        rt.spawn(discovery::run_listener(Arc::clone(&state)));

        // TCP receiver — accepts incoming file transfers.
        rt.spawn(transfer::run_receiver(Arc::clone(&state)));

        // Sender queue — dequeues send requests pushed by the UI.
        rt.spawn(transfer::run_sender_queue(Arc::clone(&state), send_rx));
    }

    // ── eframe GUI ────────────────────────────────────────────────────────────
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("LAN File Transfer")
            .with_inner_size([700.0, 520.0])
            .with_min_inner_size([540.0, 400.0]),
        ..Default::default()
    };

    eframe::run_native(
        "LAN File Transfer",
        native_options,
        Box::new(move |_cc| {
            Ok(Box::new(LanTransferApp {
                state: Arc::clone(&state),
                rt: Arc::clone(&rt),
            }))
        }),
    )
}
