//! In-memory span capture for tests.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt;

pub struct Capture {
    pub exporter: InMemorySpanExporter,
    _provider: SdkTracerProvider,
    _guard: DefaultGuard,
}

impl Capture {
    /// Thread-local subscriber exporting every span synchronously; use with a
    /// current-thread runtime.
    #[must_use]
    pub fn install() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
        let tracer = provider.tracer("test");
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(super::export_layers(&tracer, "info")),
        );
        Self { exporter, _provider: provider, _guard: guard }
    }

    #[must_use]
    pub fn spans(&self) -> Vec<SpanData> {
        self.exporter.get_finished_spans().expect("in-memory exporter")
    }

    #[must_use]
    pub fn named(&self, name: &str) -> Vec<SpanData> {
        self.spans().into_iter().filter(|s| s.name == name).collect()
    }
}

#[must_use]
pub fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().into_owned())
}
