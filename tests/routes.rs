//! DB-free behavioural coverage: every mount answers with the right status AND
//! body shape (error contracts, static payloads, graceful degradation), not just
//! non-404.

mod common;

use axum::http::StatusCode;
use common::{body_json, get, send, state};
use serde_json::Value;

#[tokio::test]
async fn health_reports_db_failure_as_500_error_shape() {
    let st = state(&[]);
    for path in ["/health", "/ws/2", "/musicbrainz/ws/2", "/music/ws/2"] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{path}");
        assert_eq!(body["error"], "internal database error", "{path}");
    }
}

#[tokio::test]
async fn health_sources_always_answers_with_report_shape() {
    let st = state(&[]);
    let (status, body) = get(&st, "/health/sources").await;
    assert_eq!(status, StatusCode::OK);
    let sources = body["sources"].as_array().expect("sources array");
    assert!(!sources.is_empty());
    for s in sources {
        assert!(s["id"].is_string(), "source entry: {s}");
        assert!(s["healthy"].is_boolean(), "source entry: {s}");
        assert!(s["reachable"].is_boolean(), "source entry: {s}");
    }
}

#[tokio::test]
async fn ws2_search_missing_query_is_400_across_mounts() {
    let st = state(&[]);
    let cases = [
        ("/ws/2/artist", "missing query"),
        ("/musicbrainz/ws/2/artist", "missing query"),
        ("/music/artist", "missing query"),
        ("/music/ws/2/artist", "missing query"),
        ("/ws/2/release", "missing release title"),
        ("/music/release", "missing release title"),
        ("/ws/2/recording", "missing recording title"),
        ("/music/recording", "missing recording title"),
    ];
    for (path, msg) in cases {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(body["error"], msg, "{path}");
    }
}

#[tokio::test]
async fn ws2_malformed_mbid_is_400() {
    let st = state(&[]);
    for path in [
        "/ws/2/artist/not-a-uuid",
        "/ws/2/release/not-a-uuid",
        "/ws/2/recording/not-a-uuid",
        "/ws/2/release-group/not-a-uuid",
        "/musicbrainz/ws/2/artist/not-a-uuid",
        "/music/release/not-a-uuid",
        "/ws/2/release?query=arid:not-a-uuid",
    ] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(msg.contains("invalid mbid"), "{path}: {msg}");
    }
}

#[tokio::test]
async fn release_group_browse_requires_artist_param() {
    let st = state(&[]);
    let (status, body) = get(&st, "/ws/2/release-group").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "missing artist");
    let (status, body) = get(&st, "/music/release-group?artist=nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("invalid mbid"));
}

#[tokio::test]
async fn wrong_method_and_unknown_route_shapes() {
    let st = state(&[]);
    let resp = send(&st, "POST", "/ws/2/artist").await;
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_json(resp).await["error"], "shirabe: method not allowed: POST /ws/2/artist");

    let resp = send(&st, "GET", "/v4/login").await;
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let resp = send(&st, "GET", "/tmdb/3/nope").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(resp).await["error"], "shirabe: no such route: GET /tmdb/3/nope");
}

#[tokio::test]
async fn tmdb_configuration_is_static_and_mounted_twice() {
    let st = state(&[]);
    for path in ["/3/configuration", "/tmdb/3/configuration"] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body["images"]["secure_base_url"], "https://image.tmdb.org/t/p/", "{path}");
        assert!(body["images"]["poster_sizes"].as_array().is_some_and(|a| !a.is_empty()));
        assert!(body["change_keys"].is_array());
    }
}

#[tokio::test]
async fn tmdb_search_error_contract() {
    let st = state(&[]);
    let (status, body) = get(&st, "/3/search/movie").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status_code"], 22);
    assert!(body["status_message"].is_string());

    let (status, body) = get(&st, "/3/search/tv?query=title!%3D%22x%22").await;
    assert_eq!(status, StatusCode::OK, "unanswerable query degrades to empty envelope");
    assert_eq!(body["results"], Value::Array(vec![]));
    assert_eq!(body["total_results"], 0);
    assert_eq!(body["page"], 1);

    let (status, body) = get(&st, "/tmdb/3/search/movie?query=zzz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "no key + no local index");
    assert_eq!(body["status_code"], 7);
}

#[tokio::test]
async fn tmdb_detail_error_contract() {
    let st = state(&[]);
    let (status, body) = get(&st, "/3/movie/not-a-number").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status_code"], 34);

    for path in ["/3/movie/603", "/3/tv/1396", "/tmdb/3/tv/1396", "/3/tv/1396/season/1"] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(body["status_code"], 7, "{path}");
        assert_eq!(body["status_message"], "TMDB source not configured", "{path}");
    }

    let (status, body) = get(&st, "/3/tv/1396/season/one").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status_code"], 34);
}

#[tokio::test]
async fn tvdb_login_and_error_contract() {
    let st = state(&[]);
    let resp = send(&st, "POST", "/v4/login").await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "failure");

    let keyed = state(&["--tvdb-api-key", "k"]);
    for path in ["/v4/login", "/tvdb/v4/login"] {
        let resp = send(&keyed, "POST", path).await;
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
        let body = body_json(resp).await;
        assert!(body["data"]["token"].is_string(), "{path}");
    }

    let (status, body) = get(&st, "/v4/search").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], "failure");

    let (status, body) = get(&st, "/v4/search?query=title!%3D%22x%22").await;
    assert_eq!(status, StatusCode::OK, "unanswerable query degrades to empty data");
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], Value::Array(vec![]));

    for path in [
        "/v4/search?query=zzz",
        "/v4/series/1396",
        "/tvdb/v4/series/1396",
        "/v4/series/1396/extended",
        "/v4/series/1396/episodes/default",
        "/v4/movies/42",
    ] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(body["status"], "failure", "{path}");
    }

    for path in [
        "/v4/series/abc",
        "/v4/series/abc/extended",
        "/v4/series/abc/episodes/default",
        "/v4/movies/abc",
    ] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(body["status"], "failure", "{path}");
    }
}

#[tokio::test]
async fn fanart_not_configured_contract_on_every_route() {
    let st = state(&[]);
    for path in [
        "/v3/music/11111111-1111-4111-8111-111111111111",
        "/v3/music/albums/11111111-1111-4111-8111-111111111111",
        "/v3/movies/603",
        "/v3/tv/81797",
        "/fanart/v3/music/11111111-1111-4111-8111-111111111111",
        "/fanart/v3/tv/81797",
    ] {
        let (status, body) = get(&st, path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(body["status"], "error", "{path}");
        assert!(body["error message"].is_string(), "{path}");
    }
}

#[tokio::test]
async fn coverart_ia_guard_rejects_private_hosts_on_both_mounts() {
    let st = state(&[]);
    for path in [
        "/_ia/127.0.0.1/some/image.jpg",
        "/_ia/10.0.0.1/some/image.jpg",
        "/coverart/_ia/127.0.0.1/some/image.jpg",
        "/_ia/localhost:9999/explicit/port.jpg",
    ] {
        let resp = send(&st, "GET", path).await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{path}");
    }
}
