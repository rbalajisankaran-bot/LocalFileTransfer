/// ui.rs — egui/eframe GUI for the LAN file transfer app.
///
/// The UI is rendered on every frame (immediate-mode style).
/// It reads and writes `SharedState` through a mutex-locked reference.
///
/// Layout:
///   ┌─────────────────────────────────────────┐
///   │ LAN File Transfer  [● Discoverable]      │
///   ├──────────────┬──────────────────────────┤
///   │ Peers        │ Send File                 │
///   │  • DeviceA   │  [Browse]  file.txt       │
///   │  • DeviceB   │  [Send →]                 │
///   ├──────────────┴──────────────────────────┤
///   │ Transfers                                │
///   │  ⬆ file.txt  DeviceB  ████░░  45% 2MB/s │
///   └─────────────────────────────────────────┘
///
/// Modal overlay for incoming file requests appears centred on the window.

use std::sync::Arc;

use eframe::egui::{self, Color32, FontId, RichText, Stroke, Vec2};

use crate::app_state::{SharedState, TransferDirection, TransferStatus};

/// Top-level application struct handed to `eframe::run_native`.
pub struct LanTransferApp {
    /// Shared state with all async tasks.
    pub state: SharedState,
    /// Tokio handle so we can spawn tasks from the sync UI context.
    pub rt: Arc<tokio::runtime::Runtime>,
}

impl eframe::App for LanTransferApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Request a repaint frequently so progress bars stay live.
        ctx.request_repaint_after(std::time::Duration::from_millis(250));

        // Prune stale peers before rendering.
        {
            let mut s = self.state.lock().unwrap();
            s.prune_stale_peers(crate::discovery::PEER_TTL_SECS);
        }

        // Render the incoming-request modal first (if present), so it's on top.
        self.render_incoming_modal(ctx);

        // Main window.
        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(Color32::from_rgb(18, 18, 28)))
            .show(ctx, |ui| {
                self.render_main(ui);
            });
    }
}

impl LanTransferApp {
    /// Main content: header, peer list, send panel, transfer list.
    fn render_main(&mut self, ui: &mut egui::Ui) {
        let spacing = ui.spacing_mut();
        spacing.item_spacing = Vec2::new(12.0, 8.0);

        // ── Header ──────────────────────────────────────────────────────────
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            ui.add_space(8.0);
            ui.label(
                RichText::new("⇄  LAN Transfer")
                    .font(FontId::proportional(22.0))
                    .color(Color32::from_rgb(140, 200, 255))
                    .strong(),
            );

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(8.0);
                // Discoverable toggle.
                let (mut discoverable, device_name) = {
                    let s = self.state.lock().unwrap();
                    (s.is_discoverable, s.device_name.clone())
                };

                let toggle_label = if discoverable { "🟢 Discoverable" } else { "🔴 Hidden" };
                let toggle_text = RichText::new(toggle_label)
                    .font(FontId::proportional(13.0))
                    .color(if discoverable {
                        Color32::from_rgb(100, 220, 130)
                    } else {
                        Color32::from_rgb(220, 100, 100)
                    });

                if ui.button(toggle_text).clicked() {
                    discoverable = !discoverable;
                    self.state.lock().unwrap().is_discoverable = discoverable;
                }

                ui.add_space(4.0);
                ui.label(
                    RichText::new(format!("💻 {}", device_name))
                        .font(FontId::proportional(12.0))
                        .color(Color32::from_rgb(160, 160, 180)),
                );
            });
        });

        ui.add_space(4.0);
        ui.separator();
        ui.add_space(4.0);

        // ── Two-column layout: Peers | Send ──────────────────────────────────
        ui.horizontal_top(|ui| {
            ui.add_space(8.0);

            // Left: Peer list ─────────────────────────────────────────────────
            ui.vertical(|ui| {
                ui.set_min_width(200.0);
                ui.label(
                    RichText::new("📡 Nearby Devices")
                        .font(FontId::proportional(14.0))
                        .color(Color32::from_rgb(180, 180, 220)),
                );
                ui.add_space(4.0);

                let (peers_snapshot, selected) = {
                    let s = self.state.lock().unwrap();
                    let peers: Vec<(String, String)> = s
                        .peers
                        .iter()
                        .map(|(k, (info, _))| (k.clone(), info.device_name.clone()))
                        .collect();
                    (peers, s.selected_peer.clone())
                };

                if peers_snapshot.is_empty() {
                    ui.label(
                        RichText::new("Scanning…")
                            .color(Color32::from_rgb(120, 120, 140))
                            .italics(),
                    );
                } else {
                    egui::ScrollArea::vertical()
                        .max_height(200.0)
                        .id_salt("peer_scroll")
                        .show(ui, |ui| {
                            for (key, name) in &peers_snapshot {
                                let is_selected = selected.as_deref() == Some(key.as_str());
                                let label = format!("{}  ({})", name, key);
                                let response = ui.selectable_label(
                                    is_selected,
                                    RichText::new(&label)
                                        .font(FontId::proportional(13.0))
                                        .color(if is_selected {
                                            Color32::from_rgb(140, 200, 255)
                                        } else {
                                            Color32::from_rgb(210, 210, 230)
                                        }),
                                );
                                if response.clicked() {
                                    self.state.lock().unwrap().selected_peer =
                                        Some(key.clone());
                                }
                            }
                        });
                }
            });

            ui.separator();

            // Right: Send panel ───────────────────────────────────────────────
            ui.vertical(|ui| {
                ui.label(
                    RichText::new("📤 Send File")
                        .font(FontId::proportional(14.0))
                        .color(Color32::from_rgb(180, 180, 220)),
                );
                ui.add_space(4.0);

                // Browse button.
                let file_label = {
                    let s = self.state.lock().unwrap();
                    match &s.selected_file {
                        Some(p) => p
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string(),
                        None => "No file selected".to_string(),
                    }
                };

                ui.horizontal(|ui| {
                    if ui
                        .button(RichText::new("📂 Browse").font(FontId::proportional(13.0)))
                        .clicked()
                    {
                        if let Some(path) = rfd::FileDialog::new().pick_file() {
                            self.state.lock().unwrap().selected_file = Some(path);
                        }
                    }
                    ui.label(
                        RichText::new(&file_label)
                            .font(FontId::proportional(12.0))
                            .color(Color32::from_rgb(180, 200, 180)),
                    );
                });

                ui.add_space(4.0);

                // Send button.
                let can_send = {
                    let s = self.state.lock().unwrap();
                    s.selected_peer.is_some() && s.selected_file.is_some()
                };

                let send_btn = egui::Button::new(
                    RichText::new("▶ Send")
                        .font(FontId::proportional(14.0))
                        .color(Color32::WHITE),
                )
                .fill(if can_send {
                    Color32::from_rgb(40, 120, 220)
                } else {
                    Color32::from_rgb(60, 60, 80)
                })
                .stroke(Stroke::NONE)
                .min_size(Vec2::new(120.0, 32.0));

                if ui.add_enabled(can_send, send_btn).clicked() {
                    let (peer_ip, peer_port, file_path) = {
                        let s = self.state.lock().unwrap();
                        let key = s.selected_peer.clone().unwrap();
                        let (info, _) = s.peers.get(&key).unwrap().clone();
                        let file = s.selected_file.clone().unwrap();
                        (info.ip.clone(), info.tcp_port, file)
                    };

                    let tx = self.state.lock().unwrap().send_tx.clone();
                    if let Some(tx) = tx {
                        let _ = tx.send((peer_ip, peer_port, file_path));
                        // Clear selected file after queueing.
                        self.state.lock().unwrap().selected_file = None;
                    }
                }
            });
        });

        ui.add_space(4.0);
        ui.separator();
        ui.add_space(4.0);

        // ── Transfer list ────────────────────────────────────────────────────
        ui.add_space(8.0);
        ui.label(
            RichText::new("📋 Transfers")
                .font(FontId::proportional(14.0))
                .color(Color32::from_rgb(180, 180, 220)),
        );
        ui.add_space(4.0);

        let transfers_snapshot = {
            self.state.lock().unwrap().transfers.clone()
        };

        if transfers_snapshot.is_empty() {
            ui.label(
                RichText::new("No transfers yet.")
                    .color(Color32::from_rgb(100, 100, 120))
                    .italics(),
            );
        } else {
            egui::ScrollArea::vertical()
                .id_salt("transfer_scroll")
                .max_height(240.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for (idx, entry) in transfers_snapshot.iter().enumerate().rev() {
                        ui.push_id(idx, |ui| {
                            render_transfer_row(ui, entry);
                        });
                    }
                });
        }

        ui.add_space(8.0);

        // ── Footer ───────────────────────────────────────────────────────────
        ui.with_layout(egui::Layout::bottom_up(egui::Align::Center), |ui| {
            ui.label(
                RichText::new("LAN Transfer • UDP:34254 TCP:34255")
                    .font(FontId::proportional(10.0))
                    .color(Color32::from_rgb(70, 70, 90)),
            );
        });
    }

    /// Modal window for incoming Accept/Reject requests.
    fn render_incoming_modal(&mut self, ctx: &egui::Context) {
        // Take any pending request to display.
        let pending = {
            let s = self.state.lock().unwrap();
            // Just peek — we'll pop after a decision.
            s.incoming_requests.first().map(|r| {
                (
                    r.id,
                    r.peer_name.clone(),
                    r.filename.clone(),
                    r.size_bytes,
                    Arc::clone(&r.decision_tx),
                )
            })
        };

        if let Some((id, peer_name, filename, size_bytes, decision_tx)) = pending {
            let size_str = humanize_bytes(size_bytes);
            let mut accepted = None;

            egui::Window::new("📥 Incoming File")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, Vec2::ZERO)
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(8.0);
                        ui.label(
                            RichText::new(format!("\"{}\" wants to send you:", peer_name))
                                .font(FontId::proportional(13.0))
                                .color(Color32::from_rgb(180, 200, 220)),
                        );
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new(&filename)
                                .font(FontId::proportional(16.0))
                                .color(Color32::WHITE)
                                .strong(),
                        );
                        ui.label(
                            RichText::new(format!("Size: {}", size_str))
                                .font(FontId::proportional(12.0))
                                .color(Color32::from_rgb(160, 160, 180)),
                        );
                        ui.add_space(12.0);
                        ui.horizontal(|ui| {
                            ui.add_space(20.0);
                            if ui
                                .button(
                                    RichText::new("✅ Accept")
                                        .font(FontId::proportional(14.0))
                                        .color(Color32::WHITE),
                                )
                                .clicked()
                            {
                                accepted = Some(true);
                            }
                            ui.add_space(8.0);
                            if ui
                                .button(
                                    RichText::new("❌ Reject")
                                        .font(FontId::proportional(14.0))
                                        .color(Color32::from_rgb(255, 100, 100)),
                                )
                                .clicked()
                            {
                                accepted = Some(false);
                            }
                        });
                        ui.add_space(8.0);
                    });
                });

            if let Some(decision) = accepted {
                // Try to send the decision. The Sender is behind an Arc so we
                // can only consume it once — use a raw pointer trick to take it.
                let _ = Arc::try_unwrap(decision_tx)
                    .map(|tx| tx.send(decision));

                // Remove the request from the list.
                let mut s = self.state.lock().unwrap();
                s.incoming_requests.retain(|r| r.id != id);
            }
        }
    }
}

/// Render a single row in the transfer list.
fn render_transfer_row(ui: &mut egui::Ui, entry: &crate::app_state::TransferEntry) {
    let direction_icon = match entry.direction {
        TransferDirection::Send => "⬆",
        TransferDirection::Receive => "⬇",
    };

    let status_color = match &entry.status {
        TransferStatus::InProgress => Color32::from_rgb(100, 180, 255),
        TransferStatus::Completed => Color32::from_rgb(80, 200, 120),
        TransferStatus::Failed(_) => Color32::from_rgb(255, 100, 100),
        TransferStatus::WaitingAccept => Color32::from_rgb(255, 200, 80),
    };

    ui.add(egui::Separator::default().horizontal().shrink(0.0));
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(direction_icon)
                .font(FontId::proportional(14.0))
                .color(status_color),
        );
        ui.label(
            RichText::new(&entry.filename)
                .font(FontId::proportional(13.0))
                .color(Color32::from_rgb(220, 220, 240)),
        );
        ui.label(
            RichText::new(format!("→ {}", entry.peer_name))
                .font(FontId::proportional(11.0))
                .color(Color32::from_rgb(140, 140, 160)),
        );

        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            match &entry.status {
                TransferStatus::Completed => {
                    ui.label(RichText::new("✓ Done").color(Color32::from_rgb(80, 200, 120)));
                }
                TransferStatus::Failed(msg) => {
                    ui.label(
                        RichText::new(format!("✗ {}", msg))
                            .color(Color32::from_rgb(255, 100, 100)),
                    );
                }
                TransferStatus::InProgress => {
                    let speed_str = humanize_speed(entry.speed_bps);
                    ui.label(
                        RichText::new(speed_str)
                            .font(FontId::proportional(11.0))
                            .color(Color32::from_rgb(160, 200, 160)),
                    );
                }
                TransferStatus::WaitingAccept => {
                    ui.label(
                        RichText::new("Waiting…")
                            .color(Color32::from_rgb(255, 200, 80)),
                    );
                }
            }
        });
    });

    // Progress bar (only for InProgress).
    if matches!(entry.status, TransferStatus::InProgress) {
        let progress = entry.progress();
        let bar = egui::ProgressBar::new(progress)
            .text(format!("{:.1}%", progress * 100.0))
            .fill(Color32::from_rgb(40, 120, 220))
            .desired_height(12.0);
        ui.add(bar);
    }
}

/// Format bytes as "1.23 MB" etc.
fn humanize_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.2} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

/// Format bytes/s as "1.2 MB/s" etc.
fn humanize_speed(bps: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    if bps >= MB {
        format!("{:.1} MB/s", bps / MB)
    } else if bps >= KB {
        format!("{:.0} KB/s", bps / KB)
    } else {
        format!("{:.0} B/s", bps)
    }
}
