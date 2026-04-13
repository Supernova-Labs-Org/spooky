//! Spooky HTTP/3 Load Balancer - Main Entry Point

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{thread, time::Duration};

use clap::Parser;
use log::{error, info, warn};

use spooky_config::validator::validate as validate_config;
use spooky_edge::{QUICListener, configure_async_runtime};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // Sets a custom config file
    #[arg(short, long)]
    config: Option<String>,
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
            "Binding port {} requires root privileges.",
            config_yaml.listen.port
        );
        std::process::exit(1);
    }

    // Initialize the Logger
    spooky_utils::logger::init_logger(
        &config_yaml.log.level,
        config_yaml.log.file.enabled,
        &config_yaml.log.file.path,
    );

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
    QUICListener::spawn_control_plane_tasks(&config_yaml, &shared_state);

    let requested_workers = config_yaml.performance.worker_threads.max(1);
    let worker_count = if requested_workers > 1 && !config_yaml.performance.reuseport {
        warn!(
            "reuseport disabled while worker_threads={} configured; running a single data-plane worker",
            requested_workers
        );
        1
    } else {
        requested_workers
    };

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
        "Data-plane workers={} reuseport={} pin_workers={}",
        sockets.len(),
        config_yaml.performance.reuseport,
        config_yaml.performance.pin_workers
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_flag = shutdown.clone();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_flag.store(true, Ordering::Relaxed);
    });

    let pin_workers = config_yaml.performance.pin_workers;
    let mut worker_handles = Vec::with_capacity(sockets.len());
    for (worker_idx, socket) in sockets.into_iter().enumerate() {
        let worker_config = config_yaml.clone();
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_shared = Arc::clone(&shared_state);
        let thread_name = format!("spooky-data-plane-{}", worker_idx);
        let handle = thread::Builder::new().name(thread_name.clone()).spawn(
            move || -> Result<(), String> {
                maybe_pin_worker(worker_idx, pin_workers);
                let mut listener = QUICListener::new_with_socket_and_shared_state(
                    worker_config,
                    socket,
                    worker_shared,
                )
                .map_err(|err| format!("worker {} listener init failed: {}", worker_idx, err))?;

                while !worker_shutdown.load(Ordering::Relaxed) {
                    listener.poll();
                }

                listener.start_draining();
                while !listener.drain_complete() {
                    listener.poll();
                }
                Ok(())
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

    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut worker_failed = false;
    for handle in worker_handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                worker_failed = true;
                error!("Worker exited with error: {}", err);
            }
            Err(_) => {
                worker_failed = true;
                error!("Worker thread panicked");
            }
        }
    }

    if worker_failed {
        std::process::exit(1);
    }
    info!("Spooky shutdown complete");
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
