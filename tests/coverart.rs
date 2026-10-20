//! Cover Art proxy behaviour against a mock upstream: redirect rewriting, disk
//! manifest/byte caching (mount-invariant across `/coverart/...` and root), and
//! the `/_ia` byte layer via the insecure test seam. DB-free.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use common::{body_bytes, send, state};
use serde_json::json;
use shirabe::AppState;
use shirabe::facades::coverart::MAX_BODY_BYTES;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn coverart_state(upstream: &str, cache_dir: &tempfile::TempDir, insecure: bool) -> Arc<AppState> {
    let dir = cache_dir.path().to_str().unwrap().to_string();
    let mut args = vec!["--coverart-upstream-base", upstream, "--coverart-cache-dir", dir.as_str()];
    if insecure {
        args.push("--coverart-insecure-ia");
    }
    state(&args)
}

#[tokio::test]
async fn redirect_location_is_rewritten_to_local_ia() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/release/33333333-3333-4333-8333-333333333333/front-500"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", "https://archive.org/download/mbid-x/front.jpg"),
        )
        .expect(2)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir, false);
    for mount in ["", "/coverart"] {
        let resp = send(
            &st,
            "GET",
            &format!("{mount}/release/33333333-3333-4333-8333-333333333333/front-500"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT, "mount {mount:?}");
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "/_ia/archive.org/download/mbid-x/front.jpg",
            "mount {mount:?}"
        );
        assert_eq!(resp.headers().get("x-cache-status").unwrap(), "MISS");
    }
}

#[tokio::test]
async fn manifest_is_cached_across_both_mount_forms() {
    let server = MockServer::start().await;
    let manifest = json!({ "images": [{ "front": true }] });
    Mock::given(method("GET"))
        .and(path("/release/33333333-3333-4333-8333-333333333333"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(manifest.clone())
                .insert_header("content-type", "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir, false);

    let first = send(&st, "GET", "/coverart/release/33333333-3333-4333-8333-333333333333").await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers().get("x-cache-status").unwrap(), "MISS");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body_bytes(first).await).unwrap(),
        manifest
    );

    let second = send(&st, "GET", "/release/33333333-3333-4333-8333-333333333333").await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.headers().get("x-cache-status").unwrap(), "HIT");
    assert_eq!(second.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body_bytes(second).await).unwrap(),
        manifest
    );
}

#[tokio::test]
async fn release_group_404_is_negative_cached() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/release-group/22222222-2222-4222-8222-222222222222"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": "not found" })))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir, false);
    let first = send(&st, "GET", "/release-group/22222222-2222-4222-8222-222222222222").await;
    assert_eq!(first.status(), StatusCode::NOT_FOUND);
    assert_eq!(first.headers().get("x-cache-status").unwrap(), "MISS");
    let second =
        send(&st, "GET", "/coverart/release-group/22222222-2222-4222-8222-222222222222").await;
    assert_eq!(second.status(), StatusCode::NOT_FOUND);
    assert_eq!(second.headers().get("x-cache-status").unwrap(), "HIT");
}

#[tokio::test]
async fn upstream_connection_failure_is_502() {
    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://127.0.0.1:1", &dir, false);
    let resp = send(&st, "GET", "/release/33333333-3333-4333-8333-333333333333").await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn ia_bytes_are_cached_mount_invariantly() {
    let server = MockServer::start().await;
    let png: &[u8] = b"\x89PNG-not-really";
    Mock::given(method("GET"))
        .and(path("/img/front.png"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(png, "image/png"))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, true);
    let host = server.uri().trim_start_matches("http://").to_string();

    let first = send(&st, "GET", &format!("/_ia/{host}/img/front.png")).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers().get("x-cache-status").unwrap(), "MISS");
    assert_eq!(first.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(body_bytes(first).await, png);

    let second = send(&st, "GET", &format!("/coverart/_ia/{host}/img/front.png")).await;
    assert_eq!(second.status(), StatusCode::OK, "nested mount must share the byte cache");
    assert_eq!(second.headers().get("x-cache-status").unwrap(), "HIT");
    assert_eq!(second.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(body_bytes(second).await, png);
}

#[tokio::test]
async fn ia_upstream_redirect_bounces_back_through_local_ia() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bounce.jpg"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "https://cdn.example.org/real.jpg"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, true);
    let host = server.uri().trim_start_matches("http://").to_string();
    let resp = send(&st, "GET", &format!("/_ia/{host}/bounce.jpg")).await;
    assert_eq!(resp.status(), StatusCode::FOUND);
    assert_eq!(resp.headers().get("location").unwrap(), "/_ia/cdn.example.org/real.jpg");
}

#[tokio::test]
async fn ia_guard_still_applies_when_insecure_seam_is_off() {
    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, false);
    let resp = send(&st, "GET", "/_ia/192.168.1.1/x.jpg").await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn ia_guard_blocks_reserved_literals_and_names_resolving_to_loopback() {
    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, false);
    for host in ["100.64.0.1", "0.0.0.0", "198.18.0.1", "240.0.0.1", "localhost"] {
        let resp = send(&st, "GET", &format!("/_ia/{host}/x.jpg")).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{host}");
        assert_eq!(body_bytes(resp).await, b"forbidden host", "{host}");
    }
}

#[tokio::test]
async fn concurrent_manifest_misses_are_single_flight() {
    let server = MockServer::start().await;
    let manifest = json!({ "images": [{ "front": true, "id": 1 }] });
    Mock::given(method("GET"))
        .and(path("/release/44444444-4444-4444-8444-444444444444"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(manifest.clone())
                .insert_header("content-type", "application/json")
                .set_delay(Duration::from_millis(200)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir, false);
    let tasks: Vec<_> = (0..8)
        .map(|i| {
            let st = Arc::clone(&st);
            let mount = if i % 2 == 0 { "" } else { "/coverart" };
            tokio::spawn(async move {
                let resp = send(
                    &st,
                    "GET",
                    &format!("{mount}/release/44444444-4444-4444-8444-444444444444"),
                )
                .await;
                (resp.status(), body_bytes(resp).await)
            })
        })
        .collect();
    for task in tasks {
        let (status, body) = task.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(), manifest);
    }
}

fn cache_entries(dir: &tempfile::TempDir) -> usize {
    std::fs::read_dir(dir.path()).unwrap().count()
}

/// One-shot HTTP/1.1 server answering any request with a chunked body of
/// `total` bytes, so the client never sees a `Content-Length`.
async fn chunked_server(total: usize) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let mut req = Vec::new();
        loop {
            let n = sock.read(&mut buf).await.unwrap();
            req.extend_from_slice(&buf[..n]);
            if n == 0 || req.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head =
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nTransfer-Encoding: chunked\r\n\r\n";
        if sock.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        let chunk = vec![b'x'; 64 * 1024];
        let mut sent = 0;
        while sent < total {
            let n = chunk.len().min(total - sent);
            let frame = format!("{n:x}\r\n");
            if sock.write_all(frame.as_bytes()).await.is_err()
                || sock.write_all(&chunk[..n]).await.is_err()
                || sock.write_all(b"\r\n").await.is_err()
            {
                return;
            }
            sent += n;
        }
        let _ = sock.write_all(b"0\r\n\r\n").await;
    });
    addr.to_string()
}

#[tokio::test]
async fn ia_body_under_cap_is_served_and_cached() {
    let server = MockServer::start().await;
    let body = vec![b'x'; 1024 * 1024];
    Mock::given(method("GET"))
        .and(path("/img/ok.png"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.clone(), "image/png"))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, true);
    let host = server.uri().trim_start_matches("http://").to_string();

    let resp = send(&st, "GET", &format!("/_ia/{host}/img/ok.png")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await.len(), body.len());
    assert_eq!(cache_entries(&dir), 2);
}

#[tokio::test]
async fn ia_content_length_over_cap_is_rejected_and_not_cached() {
    let server = MockServer::start().await;
    let body = vec![b'x'; usize::try_from(MAX_BODY_BYTES).unwrap() + 1];
    Mock::given(method("GET"))
        .and(path("/img/huge.png"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "image/png"))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, true);
    let host = server.uri().trim_start_matches("http://").to_string();

    let resp = send(&st, "GET", &format!("/_ia/{host}/img/huge.png")).await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(body_bytes(resp).await, b"upstream body too large");
    assert_eq!(cache_entries(&dir), 0);
}

#[tokio::test]
async fn ia_chunked_body_over_cap_is_aborted_and_not_cached() {
    let host = chunked_server(usize::try_from(MAX_BODY_BYTES).unwrap() + 1).await;
    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state("http://unused.invalid", &dir, true);

    let resp = send(&st, "GET", &format!("/_ia/{host}/img/stream.png")).await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(body_bytes(resp).await, b"upstream body too large");
    assert_eq!(cache_entries(&dir), 0);
}

#[tokio::test]
async fn manifest_over_cap_is_rejected_and_not_cached() {
    let server = MockServer::start().await;
    let body = vec![b' '; usize::try_from(MAX_BODY_BYTES).unwrap() + 1];
    Mock::given(method("GET"))
        .and(path("/release/55555555-5555-4555-8555-555555555555"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir, false);
    let resp = send(&st, "GET", "/release/55555555-5555-4555-8555-555555555555").await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(cache_entries(&dir), 0);
}
