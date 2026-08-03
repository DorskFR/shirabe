//! Cover Art proxy behaviour against a mock upstream: redirect rewriting, disk
//! manifest/byte caching (mount-invariant across `/coverart/...` and root), and
//! the `/_ia` byte layer via the insecure test seam. DB-free.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::{body_bytes, send, state};
use serde_json::json;
use shirabe::AppState;
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
