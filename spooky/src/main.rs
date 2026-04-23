//! Spooky HTTP/3 Load Balancer - Main Entry Point

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
mod runtime_guard;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{thread, time::Duration};

use clap::Parser;
use log::{error, info, warn};

use spooky_config::validator::validate as validate_config;
use spooky_edge::{QUICListener, SharedRuntimeState, configure_async_runtime};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // Sets a custom config file
    #[arg(short, long)]
    config: Option<String>,
}

struct IngressPacket {
    peer: SocketAddr,
    local_addr: SocketAddr,
    bytes: Vec<u8>,
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        Err(err) => {
            warn!(
                "Failed to register SIGTERM handler ({}); falling back to Ctrl+C only",
                err
            );
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
    // Parse CLI arguments
    let cli = Cli::parse();

    let config_path = cli
        .config
        .unwrap_or_else(|| "./config/config.yaml".to_string());

    // Read configuration file
    let config_yaml = match spooky_config::loader::read_config(&config_path) {
        Ok(cfg) => cfg,
        Err(err_msg) => {
            eprintln!("Error loading config: {}", err_msg);
            std::process::exit(1);
        }
    };

    // Require root only when binding a privileged port (< 1024).
    let uid = unsafe { libc::getuid() };
    if uid != 0 && config_yaml.listen.port < 1024 {
        eprintln!(
            "Binding privileged port {} requires root or CAP_NET_BIND_SERVICE. \
Use a port >= 1024 for unprivileged startup.",
            config_yaml.listen.port,
        );
        std::process::exit(1);
    }

    // Initialize the Logger
    spooky_utils::logger::init_logger(
        &config_yaml.log.level,
        config_yaml.log.file.enabled,
        &config_yaml.log.file.path,
        config_yaml.log.format == spooky_config::config::LogFormat::Json,
    );
    runtime_guard::install_panic_hook();

    // Validate Configurations
    if !validate_config(&config_yaml) {
        error!("Configuration validation failed. Exiting...");
        std::process::exit(1);
    }

    configure_async_runtime(config_yaml.performance.control_plane_threads.max(1));

    let shared_state = match QUICListener::build_shared_state(&config_yaml) {
        Ok(shared_state) => Arc::new(shared_state),
        Err(e) => {
            error!("Failed to initialize shared runtime state: {}", e);
            std::process::exit(1);
        }
    };

    let requested_workers = config_yaml.performance.worker_threads.max(1);
    let shard_count = config_yaml.performance.packet_shards_per_worker.max(1);
    let worker_count = if requested_workers > 1 && !config_yaml.performance.reuseport {
        warn!(
            "reuseport disabled while worker_threads={} configured; running a single data-plane worker",
            requested_workers
        );
        1
    } else {
        requested_workers
    };
    let effective_worker_count = worker_count.saturating_mul(shard_count);
    QUICListener::spawn_control_plane_tasks(&config_yaml, &shared_state, effective_worker_count);

    let sockets = if worker_count > 1 {
        match QUICListener::bind_reuseport_sockets(&config_yaml, worker_count) {
            Ok(sockets) => sockets,
            Err(e) => {
                error!("Failed to bind SO_REUSEPORT sockets: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        match QUICListener::bind_socket(&config_yaml, false) {
            Ok(socket) => vec![socket],
            Err(e) => {
                error!("Failed to bind UDP socket: {}", e);
                std::process::exit(1);
            }
        }
    };

    info!("Spooky is starting");
    info!(
        "Data-plane workers={} packet_shards_per_worker={} reuseport={} pin_workers={}",
        sockets.len(),
        shard_count,
        config_yaml.performance.reuseport,
        config_yaml.performance.pin_workers
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_flag = shutdown.clone();

    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        shutdown_flag.store(true, Ordering::Relaxed);
    });

    let pin_workers = config_yaml.performance.pin_workers;
    let shard_queue_capacity = config_yaml.performance.packet_shard_queue_capacity.max(1);
    let shard_queue_max_bytes = config_yaml.performance.packet_shard_queue_max_bytes.max(1);
    let mut worker_handles = Vec::with_capacity(sockets.len());
    for (worker_idx, socket) in sockets.into_iter().enumerate() {
        let worker_config = config_yaml.clone();
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_shared = Arc::clone(&shared_state);
        let thread_name = format!("spooky-data-plane-{}", worker_idx);
        let handle = thread::Builder::new().name(thread_name.clone()).spawn(
            move || -> Result<(), String> {
                if shard_count <= 1 {
                    return run_single_listener_worker(
                        worker_idx,
                        pin_workers,
                        worker_config,
                        socket,
                        worker_shared,
                        worker_shutdown,
                    );
                }

                run_sharded_listener_worker(
                    worker_idx,
                    shard_count,
                    shard_queue_capacity,
                    shard_queue_max_bytes,
                    pin_workers,
                    worker_config,
                    socket,
                    worker_shared,
                    worker_shutdown,
                )
            },
        );

        match handle {
            Ok(handle) => worker_handles.push(handle),
            Err(err) => {
                error!("Failed to spawn worker thread {}: {}", worker_idx, err);
                shutdown.store(true, Ordering::Relaxed);
                break;
            }
        }
    }

    let mut worker_failed = false;
    let mut active_worker_handles = worker_handles;
    while !shutdown.load(Ordering::Relaxed) {
        let mut idx = 0usize;
        while idx < active_worker_handles.len() {
            if !active_worker_handles[idx].is_finished() {
                idx += 1;
                continue;
            }

            let handle = active_worker_handles.swap_remove(idx);
            join_worker_handle(handle, &mut worker_failed);
            if worker_failed {
                shutdown.store(true, Ordering::Relaxed);
                break;
            }
        }

        if worker_failed {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for handle in active_worker_handles {
        join_worker_handle(handle, &mut worker_failed);
    }

    let panic_count = runtime_guard::panic_count();
    if panic_count > 0 {
        worker_failed = true;
        error!("Process captured {} panic(s) via panic hook", panic_count);
    }

    if worker_failed {
        std::process::exit(1);
    }
    info!("Spooky shutdown complete");
}

#[allow(clippy::too_many_arguments)]
fn run_sharded_listener_worker(
    worker_idx: usize,
    shard_count: usize,
    shard_queue_capacity: usize,
    shard_queue_max_bytes: usize,
    pin_workers: bool,
    worker_config: spooky_config::config::Config,
    socket: std::net::UdpSocket,
    worker_shared: Arc<SharedRuntimeState>,
    worker_shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    maybe_pin_worker(worker_idx, pin_workers);

    let local_addr = socket
        .local_addr()
        .map_err(|err| format!("worker {} local_addr failed: {}", worker_idx, err))?;

    let mut shard_handles = Vec::with_capacity(shard_count);
    let mut shard_txs: Vec<SyncSender<IngressPacket>> = Vec::with_capacity(shard_count);
    let mut shard_queue_bytes: Vec<Arc<AtomicUsize>> = Vec::with_capacity(shard_count);

    for shard_idx in 0..shard_count {
        let shard_socket = socket.try_clone().map_err(|err| {
            format!(
                "worker {} shard {} socket clone failed: {}",
                worker_idx, shard_idx, err
            )
        })?;
        let shard_config = worker_config.clone();
        let shard_shared = Arc::clone(&worker_shared);
        let shard_shutdown = Arc::clone(&worker_shutdown);
        let shard_thread_idx = worker_idx
            .saturating_mul(shard_count)
            .saturating_add(shard_idx);
        let shard_queue_bytes_counter = Arc::new(AtomicUsize::new(0));
        shard_queue_bytes.push(Arc::clone(&shard_queue_bytes_counter));

        let (tx, rx) = mpsc::sync_channel::<IngressPacket>(shard_queue_capacity);
        shard_txs.push(tx);

        let shard_name = format!("spooky-data-plane-{}-shard-{}", worker_idx, shard_idx);
        let shard_handle = thread::Builder::new()
            .name(shard_name)
            .spawn(move || -> Result<(), String> {
                maybe_pin_worker(shard_thread_idx, pin_workers);
                let mut listener = QUICListener::new_with_socket_and_shared_state(
                    shard_config,
                    shard_socket,
                    shard_shared,
                )
                .map_err(|err| {
                    format!(
                        "worker {} shard {} listener init failed: {}",
                        worker_idx, shard_idx, err
                    )
                })?;

                let idle_timeout = Duration::from_millis(10);
                while !shard_shutdown.load(Ordering::Relaxed) {
                    match rx.recv_timeout(idle_timeout) {
                        Ok(packet) => {
                            let packet_bytes = packet.bytes.len();
                            listener.process_datagram(
                                packet.peer,
                                packet.local_addr,
                                &packet.bytes,
                            );
                            release_shard_queue_bytes(
                                shard_queue_bytes_counter.as_ref(),
                                packet_bytes,
                            );
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            listener.poll_idle();
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            break;
                        }
                    }
                }

                listener.start_draining();
                while !listener.drain_complete() {
                    listener.poll_idle();
                }
                Ok(())
            })
            .map_err(|err| {
                format!(
                    "failed to spawn worker {} shard {}: {}",
                    worker_idx, shard_idx, err
                )
            })?;
        shard_handles.push(shard_handle);
    }

    let mut recv_buf = [0u8; 65_535];
    while !worker_shutdown.load(Ordering::Relaxed) {
        match socket.recv_from(&mut recv_buf) {
            Ok((len, peer)) => {
                if len == 0 {
                    continue;
                }
                let shard_idx = shard_index_for_peer(&peer, shard_count);
                let packet_len = len;
                if !try_reserve_shard_queue_bytes(
                    shard_queue_bytes[shard_idx].as_ref(),
                    packet_len,
                    shard_queue_max_bytes,
                ) {
                    worker_shared.inc_ingress_queue_drop();
                    worker_shared.inc_ingress_queue_drop_bytes(packet_len);
                    continue;
                }
                let packet = IngressPacket {
                    peer,
                    local_addr,
                    bytes: recv_buf[..len].to_vec(),
                };
                match shard_txs[shard_idx].try_send(packet) {
                    Ok(()) => {}
                    Err(TrySendError::Full(packet)) => {
                        release_shard_queue_bytes(
                            shard_queue_bytes[shard_idx].as_ref(),
                            packet.bytes.len(),
                        );
                        worker_shared.inc_ingress_queue_drop();
                        worker_shared.inc_ingress_queue_drop_bytes(packet.bytes.len());
                    }
                    Err(TrySendError::Disconnected(packet)) => {
                        release_shard_queue_bytes(
                            shard_queue_bytes[shard_idx].as_ref(),
                            packet.bytes.len(),
                        );
                        return Err(format!(
                            "worker {} shard {} dispatch channel disconnected",
                            worker_idx, shard_idx
                        ));
                    }
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(err) => {
                return Err(format!(
                    "worker {} ingress recv failed: {}",
                    worker_idx, err
                ));
            }
        }
    }

    drop(shard_txs);

    let mut shard_failed = false;
    for handle in shard_handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                shard_failed = true;
                error!("Worker {} shard exited with error: {}", worker_idx, err);
            }
            Err(_) => {
                shard_failed = true;
                error!("Worker {} shard thread panicked", worker_idx);
            }
        }
    }
    if shard_failed {
        Err(format!("worker {} had failing shard(s)", worker_idx))
    } else {
        Ok(())
    }
}

fn run_single_listener_worker(
    worker_idx: usize,
    pin_workers: bool,
    worker_config: spooky_config::config::Config,
    socket: std::net::UdpSocket,
    worker_shared: Arc<SharedRuntimeState>,
    worker_shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    maybe_pin_worker(worker_idx, pin_workers);
    let mut listener =
        QUICListener::new_with_socket_and_shared_state(worker_config, socket, worker_shared)
            .map_err(|err| format!("worker {} listener init failed: {}", worker_idx, err))?;

    while !worker_shutdown.load(Ordering::Relaxed) {
        listener.poll();
    }

    listener.start_draining();
    while !listener.drain_complete() {
        listener.poll();
    }
    Ok(())
}

fn shard_index_for_peer(peer: &SocketAddr, shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    peer.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count.max(1)
}

fn try_reserve_shard_queue_bytes(counter: &AtomicUsize, packet_bytes: usize, cap: usize) -> bool {
    loop {
        let current = counter.load(Ordering::Relaxed);
        let next = current.saturating_add(packet_bytes);
        if next > cap {
            return false;
        }
        if counter
            .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return true;
        }
    }
}

fn release_shard_queue_bytes(counter: &AtomicUsize, packet_bytes: usize) {
    loop {
        let current = counter.load(Ordering::Relaxed);
        let next = current.saturating_sub(packet_bytes);
        if counter
            .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

fn maybe_pin_worker(worker_idx: usize, pin_workers: bool) {
    if !pin_workers {
        return;
    }

    let Some(core_ids) = core_affinity::get_core_ids() else {
        warn!("Worker pinning requested but core list is unavailable");
        return;
    };

    if core_ids.is_empty() {
        warn!("Worker pinning requested but no cores were reported");
        return;
    }

    let core_id = core_ids[worker_idx % core_ids.len()];
    if !core_affinity::set_for_current(core_id) {
        warn!("Failed to pin worker {} to core {}", worker_idx, core_id.id);
    }
}

fn join_worker_handle(handle: thread::JoinHandle<Result<(), String>>, worker_failed: &mut bool) {
    match handle.join() {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            *worker_failed = true;
            error!("Worker exited with error: {}", err);
        }
        Err(payload) => {
            *worker_failed = true;
            error!(
                "Worker thread panicked: {}",
                runtime_guard::panic_payload_message(payload.as_ref())
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{release_shard_queue_bytes, shard_index_for_peer, try_reserve_shard_queue_bytes};
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn shard_index_is_stable_for_same_peer() {
        let peer: SocketAddr = "127.0.0.1:12345".parse().expect("peer addr");
        let a = shard_index_for_peer(&peer, 8);
        let b = shard_index_for_peer(&peer, 8);
        assert_eq!(a, b);
    }

    #[test]
    fn shard_index_is_within_bounds() {
        let peer: SocketAddr = "10.1.2.3:443".parse().expect("peer addr");
        let idx = shard_index_for_peer(&peer, 16);
        assert!(idx < 16);
    }

    #[test]
    fn shard_queue_byte_reservation_obeys_cap() {
        let counter = AtomicUsize::new(0);
        assert!(try_reserve_shard_queue_bytes(&counter, 10, 16));
        assert!(!try_reserve_shard_queue_bytes(&counter, 7, 16));
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[test]
    fn shard_queue_byte_release_is_saturating() {
        let counter = AtomicUsize::new(8);
        release_shard_queue_bytes(&counter, 3);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 5);
        release_shard_queue_bytes(&counter, 10);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
