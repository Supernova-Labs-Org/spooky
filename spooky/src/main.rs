//! Spooky HTTP/3 Load Balancer - Main Entry Point

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use clap::Parser;
use log::{error, info};

use spooky_config::validator::validate as validate_config;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    // Sets a custom config file
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    // Check user (only root allowed)
    let uid = unsafe { libc::getuid() };

    if uid != 0 {
        eprintln!("This program must be run as root.");
        std::process::exit(1);
    }

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

    info!("Spooky is starting");
    let mut spooky = match spooky_edge::QUICListener::new(config_yaml) {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create QUIC listener: {}", e);
            std::process::exit(1);
        }
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_flag = shutdown.clone();

    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_flag.store(true, Ordering::Relaxed);
    });

    while !shutdown.load(Ordering::Relaxed) {
        spooky.poll();
    }

    spooky.start_draining();
    while !spooky.drain_complete() {
        spooky.poll();
    }

    info!("Spooky shutdown complete");
}
