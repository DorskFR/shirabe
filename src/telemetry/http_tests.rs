use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use opentelemetry::trace::{SpanKind, Status, TraceId};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::testing::{Capture, attribute};
use crate::config::Cli;
use crate::db::{Pools, connect_lazy};
use crate::facades::coverart::CoverArtState;
use crate::sources::Registry;
use crate::sources::tvdb::TokenStore;
use crate::{AppState, build_router};

const REMOTE_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

fn state(extra: &[&str]) -> Arc<AppState> {
    let argv = ["shirabe", "--database-url", "postgres://x/x"].iter().chain(extra);
    let config = Cli::try_parse_from(argv).unwrap().config;
    let pools = Pools {
        musicbrainz: connect_lazy("postgres://x/x", 1).unwrap(),
        shirabe: None,
        imdb: None,
        tmdb: None,
        tvdb: None,
        fanart: None,
    };
    let tvdb_tokens = TokenStore::new();
    let registry = Registry::with_defaults(pools.clone(), config.clone(), tvdb_tokens.clone());
    let coverart = CoverArtState::new(&config);
    let search_cache = AppState::search_cache_for(&config);
    Arc::new(AppState { pools, config, registry, tvdb_tokens, coverart, search_cache })
}

async fn get(state: Arc<AppState>, uri: &str, traceparent: Option<&str>) -> StatusCode {
    let mut req = Request::builder().uri(uri);
    if let Some(tp) = traceparent {
        req = req.header("traceparent", tp);
    }
    let resp = build_router(state).oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    resp.status()
}

fn server_spans(capture: &Capture) -> Vec<opentelemetry_sdk::trace::SpanData> {
    capture.spans().into_iter().filter(|s| s.span_kind == SpanKind::Server).collect()
}

#[tokio::test]
async fn routed_requests_get_a_server_span_named_by_route_and_probes_none() {
    let capture = Capture::install();
    let st = state(&[]);
    assert_eq!(
        get(st.clone(), "/tmdb/3/configuration?api_key=client-key", None).await,
        StatusCode::OK
    );
    let _ = get(st.clone(), "/health", None).await;

    let spans = server_spans(&capture);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let span = &spans[0];
    assert_eq!(span.name, "GET /tmdb/3/configuration");
    assert_eq!(attribute(span, "http.route").as_deref(), Some("/tmdb/3/configuration"));
    assert_eq!(attribute(span, "http.response.status_code").as_deref(), Some("200"));
    assert_eq!(span.status, Status::Unset);
    let all = format!("{:?}", capture.spans());
    assert!(!all.contains("client-key"), "client api_key exported: {all}");
}

#[tokio::test]
async fn upstream_failure_links_server_and_client_spans_under_the_remote_trace() {
    let capture = Capture::install();
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/603"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&upstream)
        .await;
    let st = state(&["--tmdb-api-key", "server-key", "--tmdb-api-base", &upstream.uri()]);
    let traceparent = format!("00-{REMOTE_TRACE}-00f067aa0ba902b7-01");

    let status = get(st, "/3/movie/603", Some(&traceparent)).await;
    assert!(status.is_server_error(), "{status}");

    let remote = TraceId::from_hex(REMOTE_TRACE).unwrap();
    let server = server_spans(&capture).pop().expect("server span");
    assert_eq!(server.span_context.trace_id(), remote);
    assert_eq!(server.name, "GET /3/movie/{id}");
    assert!(matches!(server.status, Status::Error { .. }), "{:?}", server.status);

    let client =
        capture.spans().into_iter().find(|s| s.span_kind == SpanKind::Client).expect("client span");
    assert_eq!(client.span_context.trace_id(), remote);
    assert_eq!(attribute(&client, "http.response.status_code").as_deref(), Some("500"));
    assert!(matches!(client.status, Status::Error { .. }), "{:?}", client.status);
    let url = attribute(&client, "url.full").unwrap();
    assert!(url.contains("api_key=REDACTED"), "{url}");

    let received = upstream.received_requests().await.unwrap();
    let sent = received[0].headers.get("traceparent").expect("traceparent sent upstream");
    assert!(sent.to_str().unwrap().contains(REMOTE_TRACE), "{sent:?}");
    let all = format!("{:?}", capture.spans());
    assert!(!all.contains("server-key"), "server api_key exported: {all}");
}

#[test]
fn redact_hides_credentials_and_secret_params() {
    let url = reqwest::Url::parse("https://u:p@api.example/v3/x?api_key=a&q=b&TOKEN=c").unwrap();
    assert_eq!(
        super::client::redact(&url),
        "https://api.example/v3/x?api_key=REDACTED&q=b&TOKEN=REDACTED"
    );
}
