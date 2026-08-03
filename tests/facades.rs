//! TMDB/TVDB/fanart facade behaviour against a wiremock upstream: success
//! pass-through + image-URL rewriting, upstream-error contracts (DB-free), and
//! the cache-first round trips against real provider cache DBs (#[ignore]d;
//! `make test-integration`).

mod common;

use axum::http::StatusCode;
use common::{get, provider_db, send, state};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── TMDB ──

#[tokio::test]
async fn tmdb_movie_detail_hydrates_from_upstream_and_rewrites_posters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/603"))
        .and(query_param("api_key", "test-key"))
        .and(query_param("append_to_response", "external_ids,release_dates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 603,
            "title": "The Matrix",
            "poster_path": "/poster.jpg",
            "imdb_id": "tt0133093",
            "external_ids": { "imdb_id": "tt0133093" },
            "release_dates": { "results": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--tmdb-api-key",
        "test-key",
        "--tmdb-api-base",
        &server.uri(),
        "--caache-base-url",
        "https://img.example",
    ]);
    let (status, body) = get(&st, "/3/movie/603").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "The Matrix");
    assert_eq!(body["external_ids"]["imdb_id"], "tt0133093");
    assert_eq!(
        body["poster_path"], "https://img.example/_ia/image.tmdb.org/t/p/original/poster.jpg",
        "poster must be rewritten through the image proxy"
    );
}

#[tokio::test]
async fn tmdb_upstream_error_is_502_in_tmdb_shape() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/tv/1396"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tv/1396/season/1"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    for p in ["/3/tv/1396", "/3/tv/1396/season/1", "/tmdb/3/tv/1396"] {
        let (status, body) = get(&st, p).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{p}");
        assert_eq!(body["status_code"], 11, "{p}");
    }
}

#[tokio::test]
async fn tmdb_search_merges_live_results_into_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/search/movie"))
        .and(query_param("api_key", "test-key"))
        .and(query_param("query", "matrix"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "page": 1,
            "results": [{
                "id": 603, "title": "The Matrix", "original_title": "The Matrix",
                "release_date": "1999-03-30", "overview": "hacker", "popularity": 52.4
            }],
            "total_pages": 1, "total_results": 1
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/3/search/movie?query=matrix").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_results"], 1);
    assert_eq!(body["page"], 1);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results[0]["id"], 603);
    assert_eq!(results[0]["original_title"], "The Matrix");
    assert_eq!(results[0]["release_date"], "1999-03-30");
}

#[tokio::test]
async fn tmdb_search_upstream_failure_degrades_to_empty_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/search/tv"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/3/search/tv?query=zzz").await;
    assert_eq!(status, StatusCode::OK, "a configured key never surfaces search 5xx");
    assert_eq!(body["results"], json!([]));
    assert_eq!(body["total_results"], 0);
}

#[tokio::test]
async fn tmdb_season_success_passes_through() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/tv/1396/season/2"))
        .and(query_param("api_key", "test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "episodes": [{ "episode_number": 1, "name": "Pilot", "runtime": 47 }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/3/tv/1396/season/2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["episodes"][0]["name"], "Pilot");
}

// ── TVDB ──

fn tvdb_login_mock() -> Mock {
    Mock::given(method("POST")).and(path("/login")).respond_with(
        ResponseTemplate::new(200).set_body_json(json!({ "data": { "token": "jwt-test" } })),
    )
}

#[tokio::test]
async fn tvdb_series_detail_uses_minted_bearer_and_passes_payload() {
    let server = MockServer::start().await;
    tvdb_login_mock().expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/series/81797"))
        .and(header("authorization", "Bearer jwt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": { "id": 81797, "name": "ワンピース",
                      "image": "https://artworks.thetvdb.com/banners/posters/x.jpg" }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--tvdb-api-key",
        "real-key",
        "--tvdb-api-base",
        &server.uri(),
        "--caache-base-url",
        "https://img.example",
    ]);
    let (status, body) = get(&st, "/v4/series/81797").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["name"], "ワンピース");
    assert_eq!(
        body["data"]["image"],
        "https://img.example/_ia/artworks.thetvdb.com/banners/posters/x.jpg"
    );
}

#[tokio::test]
async fn tvdb_extended_movie_and_episodes_routes_hit_expected_upstream_paths() {
    let server = MockServer::start().await;
    tvdb_login_mock().mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/series/81797/extended"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "id": 81797, "remoteIds": [{ "id": "tt0388629", "sourceName": "IMDB" }] }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/movies/42/extended"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": { "id": 42 } })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/series/81797/episodes/default"))
        .and(query_param("page", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "episodes": [{ "id": 1 }] },
            "links": { "next": null }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tvdb-api-key", "real-key", "--tvdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/v4/series/series-81797/extended").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["remoteIds"][0]["id"], "tt0388629");

    let (status, body) = get(&st, "/tvdb/v4/movies/42").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["id"], 42);

    let (status, body) = get(&st, "/v4/series/81797/episodes/default").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["links"]["next"].is_null(), "pagination stop signal must survive");
}

#[tokio::test]
async fn tvdb_search_wraps_live_data_and_upstream_error_is_502() {
    let server = MockServer::start().await;
    tvdb_login_mock().mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/search"))
        .and(query_param("type", "series"))
        .and(query_param("query", "one piece"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{ "tvdb_id": "81797", "name": "One Piece", "aliases": ["ワンピース"] }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/series/500"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let st = state(&["--tvdb-api-key", "real-key", "--tvdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/v4/search?type=series&query=one%20piece").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"][0]["tvdb_id"], "81797");
    assert_eq!(body["data"][0]["aliases"][0], "ワンピース");

    let (status, body) = get(&st, "/v4/series/500").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["status"], "failure");
}

// ── fanart ──

#[tokio::test]
async fn fanart_success_passes_through_and_rewrites_assets() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/music/11111111-1111-4111-8111-111111111111"))
        .and(query_param("api_key", "project-key"))
        .and(query_param("client_key", "personal-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "Seaside Radio",
            "mbid_id": "11111111-1111-4111-8111-111111111111",
            "artistthumb": [
                { "id": "1", "url": "https://assets.fanart.tv/fanart/music/a/x.jpg", "likes": "3" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--fanart-api-key",
        "project-key",
        "--fanart-personal-api-key",
        "personal-key",
        "--fanart-api-base",
        &server.uri(),
        "--caache-base-url",
        "https://img.example",
    ]);
    let (status, body) = get(&st, "/v3/music/11111111-1111-4111-8111-111111111111").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Seaside Radio");
    assert_eq!(
        body["artistthumb"][0]["url"],
        "https://img.example/_ia/assets.fanart.tv/fanart/music/a/x.jpg"
    );
}

#[tokio::test]
async fn fanart_404_passes_through_and_5xx_becomes_502() {
    let server = MockServer::start().await;
    let miss = json!({ "status": "error", "error message": "id not found" });
    Mock::given(method("GET"))
        .and(path("/movies/603"))
        .respond_with(ResponseTemplate::new(404).set_body_json(miss.clone()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tv/81797"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/music/albums/11111111-1111-4111-8111-111111111111"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "albums": {} })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--fanart-api-key", "project-key", "--fanart-api-base", &server.uri()]);
    let (status, body) = get(&st, "/v3/movies/603").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, miss);

    let (status, body) = get(&st, "/fanart/v3/tv/81797").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["status"], "error");
    assert_eq!(body["error message"], "fanart.tv upstream error");

    let (status, body) = get(&st, "/v3/music/albums/11111111-1111-4111-8111-111111111111").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["albums"].is_object());
}

// ── DB-gated cache-first round trips ──

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn tmdb_detail_second_call_is_served_from_cache() {
    let db = provider_db("tmdb").await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/603"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 603, "title": "The Matrix",
            "external_ids": { "imdb_id": "tt0133093" },
            "release_dates": { "results": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--tmdb-api-key",
        "test-key",
        "--tmdb-api-base",
        &server.uri(),
        "--tmdb-database-url",
        &db,
    ]);
    for round in 1..=2 {
        let (status, body) = get(&st, "/3/movie/603").await;
        assert_eq!(status, StatusCode::OK, "round {round}");
        assert_eq!(body["title"], "The Matrix", "round {round}");
    }
    let pool = shirabe::db::connect(&db, 1).await.unwrap();
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tmdb_cache").fetch_one(&pool).await.unwrap();
    assert_eq!(rows, 1);
    pool.close().await;
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn tvdb_series_second_call_is_served_from_cache() {
    let db = provider_db("tvdb").await;
    let server = MockServer::start().await;
    tvdb_login_mock().expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/series/81797"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "id": 81797, "name": "One Piece" }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--tvdb-api-key",
        "real-key",
        "--tvdb-api-base",
        &server.uri(),
        "--tvdb-database-url",
        &db,
    ]);
    for round in 1..=2 {
        let (status, body) = get(&st, "/v4/series/81797").await;
        assert_eq!(status, StatusCode::OK, "round {round}");
        assert_eq!(body["data"]["name"], "One Piece", "round {round}");
    }
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn fanart_music_second_call_is_served_from_cache() {
    let db = provider_db("fanart").await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/music/11111111-1111-4111-8111-111111111111"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "name": "Seaside Radio" })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&[
        "--fanart-api-key",
        "project-key",
        "--fanart-api-base",
        &server.uri(),
        "--fanart-database-url",
        &db,
    ]);
    for round in 1..=2 {
        let (status, body) = get(&st, "/v3/music/11111111-1111-4111-8111-111111111111").await;
        assert_eq!(status, StatusCode::OK, "round {round}");
        assert_eq!(body["name"], "Seaside Radio", "round {round}");
    }
}

#[tokio::test]
async fn tvdb_login_route_never_proxies_client_credentials() {
    let server = MockServer::start().await;
    let st = state(&["--tvdb-api-key", "real-key", "--tvdb-api-base", &server.uri()]);
    let resp = send(&st, "POST", "/v4/login").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        server.received_requests().await.unwrap().is_empty(),
        "/v4/login must mint locally, never call upstream"
    );
}
