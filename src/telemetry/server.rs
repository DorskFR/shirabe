//! One `SERVER` span per request plus the INFO access-log line.

use std::time::Duration;

use axum::extract::MatchedPath;
use axum::http::{Request, Response};
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::{
    DefaultOnBodyChunk, DefaultOnEos, MakeSpan, OnFailure, OnRequest, OnResponse, TraceLayer,
};
use tracing::Span;
use tracing::field::Empty;

/// Probes: logged, never exported.
const UNTRACED: &[&str] = &["/health", "/health/sources"];
pub(super) const PROBE_TARGET: &str = "shirabe::probe";

pub type HttpTraceLayer = TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    ServerSpan,
    NoRequestLog,
    AccessLog,
    DefaultOnBodyChunk,
    DefaultOnEos,
    RecordFailure,
>;

/// Apply with `Router::layer` so `MatchedPath` is set per route.
#[must_use]
pub fn trace_layer() -> HttpTraceLayer {
    TraceLayer::new_for_http()
        .make_span_with(ServerSpan)
        .on_request(NoRequestLog)
        .on_response(AccessLog)
        .on_failure(RecordFailure)
}

/// The span records the path only: the query string can carry a client's own
/// provider key (`api_key`), which must never be exported.
#[derive(Clone, Copy, Debug, Default)]
pub struct ServerSpan;

impl<B> MakeSpan<B> for ServerSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let path = request.uri().path();
        let method = request.method().as_str();
        if UNTRACED.contains(&path) {
            return tracing::info_span!(
                target: PROBE_TARGET,
                "http.request",
                http.request.method = %method,
                url.path = %path,
                http.response.status_code = Empty,
            );
        }
        let route = request.extensions().get::<MatchedPath>().map(MatchedPath::as_str);
        let span = tracing::info_span!(
            target: "shirabe::http",
            "http.request",
            otel.name = %route.map_or_else(|| method.to_owned(), |r| format!("{method} {r}")),
            otel.kind = "server",
            otel.status_code = Empty,
            otel.status_description = Empty,
            error.type = Empty,
            http.request.method = %method,
            http.route = route,
            url.path = %path,
            http.response.status_code = Empty,
        );
        super::set_remote_parent(&span, request.headers());
        span
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoRequestLog;

impl<B> OnRequest<B> for NoRequestLog {
    fn on_request(&mut self, _: &Request<B>, _: &Span) {}
}

/// `method`, `path`, `status`, `latency` at INFO, one line per request.
#[derive(Clone, Copy, Debug, Default)]
pub struct AccessLog;

impl<B> OnResponse<B> for AccessLog {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        let status = response.status();
        span.record("http.response.status_code", i64::from(status.as_u16()));
        if status.is_server_error() {
            span.record("otel.status_code", "ERROR");
            span.record("error.type", status.as_str());
        }
        let latency = format!("{} ms", latency.as_millis());
        tracing::info!(
            target: "shirabe::access",
            parent: span,
            status = status.as_u16(),
            latency,
            "finished processing request"
        );
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecordFailure;

impl<C: std::fmt::Display> OnFailure<C> for RecordFailure {
    fn on_failure(&mut self, failure: C, _latency: Duration, span: &Span) {
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_description", tracing::field::display(&failure));
    }
}
