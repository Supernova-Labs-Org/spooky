use std::{
    fs::{OpenOptions, create_dir_all},
    io::Write,
    path::Path,
};

use env_logger::{Builder, Target};
use log::LevelFilter;

pub fn init_logger(log_level: &str, log_enabled: bool, log_file: &str, json: bool) {
    let level = match log_level.to_lowercase().as_str() {
        "whisper" => LevelFilter::Trace,
        "haunt" => LevelFilter::Debug,
        "spooky" => LevelFilter::Info,
        "scream" => LevelFilter::Warn,
        "poltergeist" => LevelFilter::Error,
        "silence" => LevelFilter::Off,

        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        "off" => LevelFilter::Off,

        _ => {
            eprintln!(
                "Invalid log level '{}', defaulting to 'spooky' (info)",
                log_level
            );
            LevelFilter::Info
        }
    };

    let mut builder = Builder::new();
    builder.filter_level(level);

    if json {
        builder.format(|buf, record| {
            let ts = buf.timestamp_seconds();
            let level = record.level().as_str().to_ascii_lowercase();
            let msg = record.args().to_string();
            // Escape the message string minimally so it stays valid JSON.
            let escaped = msg.replace('\\', "\\\\").replace('"', "\\\"");
            writeln!(
                buf,
                "{{\"ts\":\"{ts}\",\"level\":\"{level}\",\"msg\":\"{escaped}\"}}"
            )
        });
    } else {
        builder.format_timestamp_secs();
    }

    // only write to file if enabled
    if log_enabled {
        if let Some(parent) = Path::new(log_file).parent()
            && let Err(err) = create_dir_all(parent)
        {
            eprintln!(
                "Failed to create log directory '{}': {}. Falling back to stderr logging.",
                parent.display(),
                err
            );
            builder.init();
            return;
        }

        let file = match OpenOptions::new().create(true).append(true).open(log_file) {
            Ok(file) => file,
            Err(err) => {
                eprintln!(
                    "Failed to open log file '{}': {}. Falling back to stderr logging.",
                    log_file, err
                );
                builder.init();
                return;
            }
        };

        builder.target(Target::Pipe(Box::new(file)));
    }
    // else → default (stderr)

    builder.init();
}
