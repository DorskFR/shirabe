//! Tracing subscriber with opt-in OTLP/HTTP trace export.
//!
//! Export is on only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-specific
//! variant) is set and `OTEL_SDK_DISABLED` is not `true`; the standard `OTEL_*`
//! variables configure the exporter, sampler and resource.

pub mod client;
pub mod server;
pub mod sqlx;

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use tracing::{Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const SERVICE_NAME: &str = "shirabe";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Flushes and shuts the tracer provider down on drop, waiting at most
/// [`SHUTDOWN_TIMEOUT`].
#[must_use = "dropping the guard shuts the trace exporter down"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    #[must_use]
    pub const fn exporting(&self) -> bool {
        self.provider.is_some()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        let Some(provider) = self.provider.take() else { return };
        // The async batch processor blocks on the runtime during shutdown, so the
        // wait is bounded from a side thread instead.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(provider.shutdown());
        });
        match rx.recv_timeout(SHUTDOWN_TIMEOUT) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!(%err, "trace exporter shutdown failed"),
            Err(_) => tracing::warn!(?SHUTDOWN_TIMEOUT, "trace exporter shutdown timed out"),
        }
    }
}

/// Installs the global subscriber: `RUST_LOG` (else `default_filter`) + fmt,
/// plus span export and the sqlx statement bridge when OTLP is configured.
pub fn init(default_filter: &str, ansi: bool) -> TelemetryGuard {
    let (provider, build_error) = if exporter_requested(|key| std::env::var(key).ok()) {
        build_provider(None).map_or_else(|err| (None, Some(err)), |p| (Some(p), None))
    } else {
        (None, None)
    };
    // Not an `Option` layer nor a boxed subscriber: neither forwards
    // `on_register_dispatch`, which the sqlx bridge needs.
    let _ = provider.as_ref().map_or_else(
        || tracing_subscriber::registry().with(fmt_layer(default_filter, ansi)).try_init(),
        |p| exporting_subscriber(&p.tracer(SERVICE_NAME), default_filter, ansi).try_init(),
    );
    if let Some(err) = build_error {
        tracing::warn!(%err, "OTLP trace exporter could not be built; trace export disabled");
    }
    if provider.is_some() {
        tracing::info!("OTLP trace export enabled");
    }
    TelemetryGuard { provider }
}

/// fmt logs plus span export: the stack [`init`] installs when exporting.
#[must_use]
pub fn exporting_subscriber(
    tracer: &Tracer,
    default_filter: &str,
    ansi: bool,
) -> impl Subscriber + Send + Sync + use<> {
    tracing_subscriber::registry()
        .with(fmt_layer(default_filter, ansi))
        .with(export_layers(tracer, default_filter))
}

fn fmt_layer<S>(default_filter: &str, ansi: bool) -> impl Layer<S> + use<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer().with_ansi(ansi).with_filter(env_filter(default_filter))
}

/// The span-export layers: `tracing-opentelemetry` over the log filter widened
/// to every shirabe span, and the sqlx statement bridge.
#[must_use]
pub fn export_layers<S>(tracer: &Tracer, default_filter: &str) -> impl Layer<S> + use<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_opentelemetry::layer()
        .with_tracer(tracer.clone())
        .with_filter(export_filter(default_filter))
        .and_then(
            sqlx::StatementSpans::new(tracer.clone())
                .with_filter(sqlx::statement_filter(export_filter(default_filter))),
        )
}

/// Marks `span` failed; the span must declare `otel.status_code` and
/// `otel.status_description` as empty fields.
pub fn record_error(span: &Span, err: &dyn std::fmt::Display) {
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", tracing::field::display(err));
}

/// Marks the current span failed when `result` is an error, then returns it.
pub fn traced_result<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<T, E> {
    if let Err(err) = &result {
        record_error(&Span::current(), err);
    }
    result
}

/// The W3C `traceparent` / `tracestate` pair of the current span, if any.
#[must_use]
pub fn current_trace_headers() -> HashMap<String, String> {
    let cx = Span::current().context();
    let mut carrier = HashMap::with_capacity(2);
    if cx.span().span_context().is_valid() {
        TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    }
    carrier
}

/// Parents `span` on a W3C trace context carried in `headers`.
pub fn set_remote_parent(span: &Span, headers: &axum::http::HeaderMap) {
    let cx = TraceContextPropagator::new().extract(&HeaderExtractor(headers));
    if cx.span().span_context().is_valid() {
        let _ = span.set_parent(cx);
    }
}

struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

fn exporter_requested(var: impl Fn(&str) -> Option<String>) -> bool {
    let set = |key: &str| var(key).is_some_and(|v| !v.trim().is_empty());
    let disabled = var("OTEL_SDK_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    !disabled && (set("OTEL_EXPORTER_OTLP_ENDPOINT") || set("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
}

/// `endpoint` overrides the `OTEL_EXPORTER_OTLP_*` endpoint variables.
fn build_provider(
    endpoint: Option<&str>,
) -> Result<SdkTracerProvider, opentelemetry_otlp::ExporterBuildError> {
    let mut exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(crate::http::client());
    if let Some(endpoint) = endpoint {
        exporter = exporter.with_endpoint(endpoint);
    }
    let exporter = exporter.build()?;
    let processor =
        BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build();
    Ok(SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource())
        .build())
}

fn resource() -> Resource {
    let mut builder = Resource::builder()
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        builder = builder.with_service_name(SERVICE_NAME);
    }
    builder.build()
}

fn env_filter(default_filter: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter))
}

/// Every shirabe span is exported whatever the log filter says; probes never are.
fn export_filter(default_filter: &str) -> EnvFilter {
    let probe = format!("{}=off", server::PROBE_TARGET);
    ["shirabe=info", probe.as_str()].iter().fold(env_filter(default_filter), |filter, directive| {
        filter.add_directive(directive.parse().expect("static directive"))
    })
}

#[cfg(test)]
pub mod testing;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod http_tests;
