use std::sync::OnceLock;

use log::{info, warn};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

pub fn init_tracing(
    enabled: bool,
    service_name: &str,
    otlp_endpoint: Option<&str>,
    sample_ratio: f64,
) {
    if !enabled {
        return;
    }

    let ratio = sample_ratio.clamp(0.0, 1.0);
    let endpoint = otlp_endpoint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http://127.0.0.1:4317");

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(err) => {
            warn!(
                "OpenTelemetry tracing enabled but exporter initialization failed (endpoint={}): {}",
                endpoint, err
            );
            return;
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::TraceIdRatioBased(ratio))
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", service_name.to_string())])
                .build(),
        )
        .build();

    let tracer = provider.tracer("spooky");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(otel_layer);
    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        warn!(
            "OpenTelemetry tracing setup failed to install tracing subscriber: {}",
            err
        );
        let _ = provider.shutdown();
        return;
    }

    if TRACER_PROVIDER.set(provider).is_err() {
        warn!("OpenTelemetry tracing already initialized; reusing existing tracer provider");
        return;
    }

    info!(
        "OpenTelemetry tracing enabled (service_name={}, endpoint={}, sample_ratio={})",
        service_name, endpoint, ratio
    );
}

pub fn shutdown_tracing() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}
