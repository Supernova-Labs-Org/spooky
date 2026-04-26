use std::{
    fs::{OpenOptions, create_dir_all},
    io::Write,
    path::Path,
};

use env_logger::{Builder, Target};
use log::LevelFilter;
use serde_json::json;

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
            let message = record.args().to_string();
            let mut payload = json!({
                "ts": buf.timestamp_seconds().to_string(),
                "level": record.level().as_str().to_ascii_lowercase(),
                "target": record.target(),
                "msg": message,
            });
            if let Some(value) = extract_kv_field(&message, "request_id") {
                payload["request_id"] = json!(value);
            }
            if let Some(value) = extract_kv_field(&message, "trace_id") {
                payload["trace_id"] = json!(value);
            }
            if let Some(value) = extract_kv_field(&message, "span_id") {
                payload["span_id"] = json!(value);
            }
            if let Some(value) = extract_kv_field(&message, "status") {
                payload["status"] = json!(value);
            }
            if let Some(value) = extract_kv_field(&message, "method") {
                payload["method"] = json!(value);
            }
            if let Some(value) = extract_kv_field(&message, "path") {
                payload["path"] = json!(value);
            }

            writeln!(buf, "{payload}")
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

fn extract_kv_field(message: &str, key: &str) -> Option<String> {
    let pattern = format!("{key}=");
    let start = message.find(&pattern)?;
    let value_start = start + pattern.len();
    let value = &message[value_start..];
    let end = value.find(char::is_whitespace).unwrap_or(value.len());
    let candidate = value[..end].trim_matches('"').trim_matches(',');
    if candidate.is_empty() {
        return None;
    }
    Some(candidate.to_string())
}
