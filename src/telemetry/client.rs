//! `CLIENT` spans and W3C trace propagation for outbound HTTP.

use std::future::Future;

use axum::http::{HeaderName, HeaderValue};
use tracing::field::Empty;
use tracing::{Instrument, Span};

/// Query parameters whose values never leave the process in a span.
const SECRET_PARAMS: &[&str] = &["api_key", "apikey", "key", "token", "access_token", "pin"];

/// `send()` inside a client span that carries the W3C trace context upstream.
pub trait TracedSend {
    fn send_traced(self) -> impl Future<Output = reqwest::Result<reqwest::Response>> + Send;
}

impl TracedSend for reqwest::RequestBuilder {
    fn send_traced(self) -> impl Future<Output = reqwest::Result<reqwest::Response>> + Send {
        send_traced(self)
    }
}

async fn send_traced(builder: reqwest::RequestBuilder) -> reqwest::Result<reqwest::Response> {
    let (client, request) = builder.build_split();
    let mut request = request?;
    let method = request.method().as_str().to_owned();
    let url = request.url();
    let host = url.host_str().unwrap_or_default().to_owned();
    let span = tracing::info_span!(
        target: "shirabe::http",
        "http.client",
        otel.name = %format_args!("{method} {host}"),
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_description = Empty,
        error.type = Empty,
        http.request.method = %method,
        server.address = %host,
        server.port = url.port_or_known_default().map(i64::from),
        url.full = %redact(url),
        http.response.status_code = Empty,
    );
    async move {
        for (name, value) in super::current_trace_headers() {
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(&value))
            {
                request.headers_mut().insert(name, value);
            }
        }
        let span = Span::current();
        match client.execute(request).await {
            Ok(response) => {
                let status = response.status();
                span.record("http.response.status_code", i64::from(status.as_u16()));
                if status.is_server_error() {
                    span.record("otel.status_code", "ERROR");
                    span.record("error.type", status.as_str());
                }
                Ok(response)
            }
            Err(err) => {
                span.record("error.type", if err.is_timeout() { "timeout" } else { "transport" });
                super::record_error(&span, &redacted_error(&err));
                Err(err)
            }
        }
    }
    .instrument(span)
    .await
}

fn redacted_error(err: &reqwest::Error) -> String {
    let message = err.to_string();
    err.url().map_or_else(|| message.clone(), |url| message.replace(url.as_str(), &redact(url)))
}

/// The URL with credentials and secret query values replaced by `REDACTED`.
#[must_use]
pub fn redact(url: &reqwest::Url) -> String {
    let mut url = url.clone();
    let _ = url.set_password(None);
    let _ = url.set_username("");
    if url.query().is_some() {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                let secret = SECRET_PARAMS.iter().any(|s| k.eq_ignore_ascii_case(s));
                (k.into_owned(), if secret { "REDACTED".to_owned() } else { v.into_owned() })
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.into()
}
