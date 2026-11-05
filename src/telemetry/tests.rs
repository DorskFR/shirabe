use std::collections::HashMap;

use opentelemetry::trace::{SpanKind, Status};

use super::testing::{Capture, attribute};

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> =
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
    move |key| map.get(key).cloned()
}

#[test]
fn exporter_requested_only_with_an_endpoint_and_not_disabled() {
    assert!(!super::exporter_requested(env(&[])));
    assert!(!super::exporter_requested(env(&[("OTEL_EXPORTER_OTLP_ENDPOINT", " ")])));
    assert!(super::exporter_requested(env(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318")])));
    assert!(super::exporter_requested(env(&[(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://c:4318/v1/traces"
    )])));
    assert!(!super::exporter_requested(env(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318"),
        ("OTEL_SDK_DISABLED", "TRUE"),
    ])));
}

#[test]
fn trace_headers_round_trip_through_a_remote_parent() {
    let capture = Capture::install();
    let headers = {
        let _enter = tracing::info_span!("shirabe.origin").entered();
        super::current_trace_headers()
    };
    let traceparent = headers.get("traceparent").expect("traceparent injected").clone();

    let mut map = axum::http::HeaderMap::new();
    map.insert("traceparent", traceparent.parse().unwrap());
    let child = tracing::info_span!("shirabe.child");
    super::set_remote_parent(&child, &map);
    drop(child);

    let origin = &capture.named("shirabe.origin")[0];
    let child = &capture.named("shirabe.child")[0];
    assert_eq!(child.span_context.trace_id(), origin.span_context.trace_id());
    assert_eq!(child.parent_span_id, origin.span_context.span_id());
}

#[test]
fn no_trace_headers_outside_a_span() {
    let _capture = Capture::install();
    assert!(super::current_trace_headers().is_empty());
}

#[test]
fn traced_result_marks_the_current_span_failed() {
    let capture = Capture::install();
    {
        let span = tracing::info_span!(
            "shirabe.failing",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );
        let _enter = span.enter();
        let _ = super::traced_result::<(), _>(Err("boom"));
    }
    let span = &capture.named("shirabe.failing")[0];
    assert!(matches!(&span.status, Status::Error { description } if description == "boom"));
    assert_eq!(span.span_kind, SpanKind::Internal);
    assert_eq!(attribute(span, "otel.status_code"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spans_reach_an_otlp_http_collector_when_the_guard_drops() {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/traces"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&collector)
        .await;
    let provider =
        super::build_provider(Some(&format!("{}/v1/traces", collector.uri()))).expect("provider");
    let tracer = provider.tracer("test");
    {
        let subscriber = tracing_subscriber::registry().with(super::export_layers(&tracer, "info"));
        let _default = tracing::subscriber::set_default(subscriber);
        let _span = tracing::info_span!("shirabe.e2e_probe").entered();
    }
    drop(super::TelemetryGuard { provider: Some(provider) });

    let received = collector.received_requests().await.unwrap();
    assert_eq!(received.len(), 1, "one batched export on shutdown");
    let request = &received[0];
    let content_type = request.headers.get("content-type").unwrap().to_str().unwrap();
    assert_eq!(content_type, "application/x-protobuf");
    let body = &request.body;
    assert!(body.windows(17).any(|w| w == b"shirabe.e2e_probe"), "span name in payload");
    assert!(body.windows(7).any(|w| w == b"shirabe"), "service name in payload");
}
