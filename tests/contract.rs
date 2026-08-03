//! Pins the request shapes downstream clients send and the load-bearing
//! response fields they deserialize, per facade. MB tier is DB-gated
//! (#[ignore]; `make test-integration`), the rest runs DB-free via wiremock.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use common::{
    ARTIST_MBID, RECORDING_MBID, RELEASE_GROUP_MBID, RELEASE_MBID, body_bytes, body_json, get,
    mb_fixture_db, send, state, state_with_db,
};
use serde_json::{Value, json};
use shirabe::{AppState, build_router};
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ARTIST2_MBID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const RELEASE_GROUP2_MBID: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

async fn send_json(st: &Arc<AppState>, method: &str, path: &str, body: &Value) -> Response {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    build_router(st.clone()).oneshot(req).await.unwrap()
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn mb_search_surface_matches_downstream_query_shapes() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) = get(&st, "/ws/2/artist?query=seaside%20radio&fmt=json&limit=15").await;
    assert_eq!(status, StatusCode::OK);
    let artist = &body["artists"][0];
    assert_eq!(artist["id"], ARTIST_MBID);
    assert_eq!(artist["name"], "Seaside Radio");
    assert!(artist["score"].is_i64() || artist["score"].is_u64());
    assert_eq!(artist["aliases"][0]["name"], "Régio Costera", "aliases feed confidence scoring");

    let (status, body) = get(&st, "/ws/2/artist?query=seaside~&fmt=json&limit=15").await;
    assert_eq!(status, StatusCode::OK, "Lucene fuzzy suffix must not break matching");
    assert_eq!(body["artists"][0]["id"], ARTIST_MBID);

    let (status, body) = get(
        &st,
        "/ws/2/release?query=release:(harbour%20lights)%20AND%20artist:(seaside%20radio)\
         %20AND%20date:(1997*)&fmt=json&limit=15",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fielded release+artist+date search");
    assert_eq!(body["count"], 1, "date:(1997*) narrows to the dated edition");
    let release = &body["releases"][0];
    assert_eq!(release["id"], RELEASE_MBID);
    assert_eq!(release["title"], "Harbour Lights");
    assert_eq!(release["status"], "Official");
    assert_eq!(release["date"], "1997-05-21");
    assert_eq!(release["artist-credit"][0]["artist"]["id"], ARTIST_MBID);
    assert_eq!(
        release["release-group"]["id"], RELEASE_GROUP_MBID,
        "release-group.id drives edition de-duplication downstream"
    );

    let (status, body) = get(
        &st,
        &format!(
            "/ws/2/release?query=arid:{ARTIST_MBID}%20AND%20primarytype:album\
             %20AND%20status:official&fmt=json&limit=100"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "arid+primarytype+status browse-by-query");
    assert_eq!(body["count"], 2);
    for release in body["releases"].as_array().unwrap() {
        assert_eq!(release["release-group"]["id"], RELEASE_GROUP_MBID);
        assert_eq!(release["status"], "Official");
    }

    let (status, body) = get(
        &st,
        "/ws/2/recording?query=recording:%22lighthouse%20keeper%22%20AND%20\
         artist:%22seaside%20radio%22&fmt=json&limit=15&inc=releases%2Bartist-credits%2Bmedia",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "fielded recording search");
    assert_eq!(body["count"], 1);
    let rec = &body["recordings"][0];
    assert_eq!(rec["id"], RECORDING_MBID);
    assert_eq!(rec["title"], "Lighthouse Keeper");
    assert_eq!(rec["length"], 387_000);
    assert_eq!(rec["artist-credit"][0]["artist"]["id"], ARTIST_MBID);
    let nested = &rec["releases"][0];
    assert_eq!(nested["id"], RELEASE_MBID);
    assert_eq!(nested["status"], "Official", "status picks the canonical release downstream");
    assert_eq!(nested["date"], "1997-05-21");
    assert_eq!(nested["release-group"]["id"], RELEASE_GROUP_MBID);
    assert_eq!(nested["media"][0]["track-count"], 2);
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn mb_lookup_surface_matches_downstream_inc_shapes() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) = get(&st, &format!("/ws/2/artist/{ARTIST_MBID}?fmt=json")).await;
    assert_eq!(status, StatusCode::OK, "bare artist lookup is the connection probe");
    assert_eq!(body["id"], ARTIST_MBID);
    assert_eq!(body["name"], "Seaside Radio");

    let (status, body) = get(
        &st,
        &format!("/ws/2/artist/{ARTIST_MBID}?fmt=json&inc=genres%2Btags%2Burl-rels%2Bannotation"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["type"], "Group");
    assert_eq!(body["disambiguation"], "test band");
    assert_eq!(body["annotation"], "seaside radio annotation");
    assert_eq!(body["genres"][0]["name"], "rock");
    assert_eq!(body["tags"][0]["name"], "rock", "tags are the genre fallback downstream");
    assert_eq!(body["relations"][0]["type"], "official homepage");
    assert_eq!(body["relations"][0]["url"]["resource"], "https://seaside-radio.example");

    let (status, body) = get(
        &st,
        &format!(
            "/ws/2/release/{RELEASE_MBID}?fmt=json&inc=aliases%2Bartist-credits%2Bmedia\
             %2Brecordings%2Brecording-rels%2Brelease-rels%2Bgenres"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "full release lookup");
    assert_eq!(body["id"], RELEASE_MBID);
    assert_eq!(body["title"], "Harbour Lights");
    assert_eq!(body["date"], "1997-05-21");
    assert_eq!(body["status"], "Official");
    assert_eq!(body["disambiguation"], "deluxe edition");
    assert_eq!(body["artist-credit"][0]["artist"]["id"], ARTIST_MBID);
    assert_eq!(body["release-group"]["id"], RELEASE_GROUP_MBID);
    assert_eq!(body["track-count"], 2);
    let medium = &body["media"][0];
    assert_eq!(medium["position"], 1);
    assert_eq!(medium["track-count"], 2);
    let tracks = medium["tracks"].as_array().expect("media[].tracks[]");
    assert_eq!(tracks.len(), 2);
    assert_eq!(tracks[0]["position"], 1);
    assert_eq!(tracks[0]["title"], "Foghorn Morning");
    assert_eq!(tracks[0]["recording"]["id"], "55555555-5555-4555-8555-555555555555");
    assert_eq!(tracks[1]["recording"]["id"], RECORDING_MBID);
    assert_eq!(tracks[1]["recording"]["length"], 387_000);

    let (status, body) = get(
        &st,
        &format!(
            "/ws/2/recording/{RECORDING_MBID}?fmt=json&inc=releases%2Bartist-credits%2Baliases"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recording lookup");
    assert_eq!(body["id"], RECORDING_MBID);
    assert_eq!(body["title"], "Lighthouse Keeper");
    assert_eq!(body["artist-credit"][0]["artist"]["aliases"][0]["name"], "Régio Costera");
    assert_eq!(body["releases"][0]["id"], RELEASE_MBID);

    let (status, body) = get(
        &st,
        &format!("/ws/2/release-group/{RELEASE_GROUP_MBID}?fmt=json&inc=artist-credits%2Breleases"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "release-group lookup");
    assert_eq!(body["id"], RELEASE_GROUP_MBID);
    assert_eq!(body["title"], "Harbour Lights");
    assert_eq!(body["primary-type"], "Album");
    assert_eq!(body["secondary-types"][0], "Live");
    assert_eq!(body["first-release-date"], "1997-05-21");
    let releases = body["releases"].as_array().expect("releases[]");
    assert!(
        !releases.is_empty(),
        "an empty releases[] hard-fails the add-from-discography action downstream"
    );
    for release in releases {
        assert!(release["id"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(release["status"], "Official");
    }
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn mb_release_group_browse_emits_count_and_honours_paging() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) = get(
        &st,
        &format!(
            "/ws/2/release-group?artist={ARTIST2_MBID}&limit=100&offset=0&fmt=json\
             &inc=artist-credits"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["release-group-count"], 1,
        "release-group-count terminates the paged walk; omitting it truncates discographies"
    );
    assert_eq!(body["release-group-offset"], 0);
    let rg = &body["release-groups"][0];
    assert_eq!(rg["id"], RELEASE_GROUP2_MBID);
    assert_eq!(rg["title"], "Peat and Heather");
    assert_eq!(rg["primary-type"], "Album");
    assert!(rg["secondary-types"].is_array());
    assert_eq!(rg["first-release-date"], "2003-04");
    assert_eq!(rg["artist-credit"][0]["artist"]["id"], ARTIST2_MBID);

    let (status, body) =
        get(&st, &format!("/ws/2/release-group?artist={ARTIST2_MBID}&limit=100&offset=100")).await;
    assert_eq!(status, StatusCode::OK, "a past-the-end page must not error (it aborts the walk)");
    assert_eq!(body["release-group-count"], 1);
    assert_eq!(body["release-groups"], json!([]));
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn mb_dates_serialize_in_all_partial_forms() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let dates_of = |body: &Value| -> Vec<String> {
        body["releases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["date"].as_str().unwrap().to_string())
            .collect()
    };

    let (status, body) = get(&st, "/ws/2/release?query=harbour%20lights&fmt=json").await;
    assert_eq!(status, StatusCode::OK);
    let dates = dates_of(&body);
    assert!(dates.contains(&"1997-05-21".to_string()), "YYYY-MM-DD: {dates:?}");
    assert!(dates.contains(&"1998".to_string()), "YYYY: {dates:?}");

    let (status, body) = get(&st, "/ws/2/release?query=peat%20and%20heather&fmt=json").await;
    assert_eq!(status, StatusCode::OK);
    let dates = dates_of(&body);
    assert!(dates.contains(&"2003-04".to_string()), "YYYY-MM: {dates:?}");
    assert!(dates.contains(&String::new()), "empty date: {dates:?}");
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn mb_browse_tolerates_sequential_paged_walk() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    for offset in (0..500).step_by(100) {
        let (status, body) = get(
            &st,
            &format!(
                "/ws/2/release-group?artist={ARTIST_MBID}&limit=100&offset={offset}&fmt=json\
                 &inc=artist-credits"
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "offset {offset}");
        assert_eq!(body["release-group-count"], 1, "offset {offset}");
    }
}

fn coverart_state(upstream: &str, cache_dir: &tempfile::TempDir) -> Arc<AppState> {
    let dir = cache_dir.path().to_str().unwrap().to_string();
    state(&[
        "--coverart-upstream-base",
        upstream,
        "--coverart-cache-dir",
        dir.as_str(),
        "--coverart-insecure-ia",
    ])
}

#[tokio::test]
async fn caa_front_500_resolves_to_image_bytes_through_both_layers() {
    let server = MockServer::start().await;
    let host = server.uri().trim_start_matches("http://").to_string();
    let jpeg: &[u8] = b"\xff\xd8\xff\xe0-not-a-real-jpeg";
    Mock::given(method("GET"))
        .and(path(format!("/release/{RELEASE_MBID}/front-500")))
        .respond_with(ResponseTemplate::new(307).insert_header(
            "location",
            format!("http://{host}/download/mbid-{RELEASE_MBID}/front-500.jpg").as_str(),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/download/mbid-{RELEASE_MBID}/front-500.jpg")))
        .respond_with(ResponseTemplate::new(200).set_body_raw(jpeg, "image/jpeg"))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir);

    let resp = send(&st, "GET", &format!("/coverart/release/{RELEASE_MBID}/front-500")).await;
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    let location = resp.headers().get("location").unwrap().to_str().unwrap().to_string();
    assert_eq!(location, format!("/_ia/{host}/download/mbid-{RELEASE_MBID}/front-500.jpg"));

    let resp = send(&st, "GET", &location).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.starts_with("image/"), "downstream asserts the image/* prefix, got {ct}");
    assert_eq!(body_bytes(resp).await, jpeg);
}

#[tokio::test]
async fn caa_upstream_404_is_an_authoritative_miss() {
    let server = MockServer::start().await;
    let miss = json!({ "error": "No cover art found for release" });
    Mock::given(method("GET"))
        .and(path(format!("/release/{RELEASE_MBID}/front-500")))
        .respond_with(ResponseTemplate::new(404).set_body_json(miss.clone()))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let st = coverart_state(&server.uri(), &dir);

    // 404 passes through the redirect layer (downstream negative-caches it) and
    // is itself negative-cached, so the second round must not re-hit upstream.
    for round in ["MISS", "HIT"] {
        let resp = send(&st, "GET", &format!("/coverart/release/{RELEASE_MBID}/front-500")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "round {round}");
        assert_eq!(resp.headers().get("x-cache-status").unwrap(), round);
        let body: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, miss, "round {round}");
    }
}

#[tokio::test]
async fn fanart_endpoints_pass_through_the_exact_artwork_keys() {
    let artist = ARTIST_MBID;
    let album = RELEASE_GROUP_MBID;
    let entry = |url: &str, likes: Value| json!({ "id": "10714", "url": url, "likes": likes });
    // likes arrives as string or int upstream; both forms must survive.
    let cases: Vec<(String, String, Value)> = vec![
        (
            format!("/v3/music/{artist}"),
            format!("/music/{artist}"),
            json!({
                "name": "Seaside Radio",
                "mbid_id": artist,
                "artistthumb": [entry("https://assets.fanart.tv/fanart/music/a/thumb.jpg",
                                      json!("3"))],
                "artistbackground": [entry("https://assets.fanart.tv/fanart/music/a/bg.jpg",
                                           json!(7))],
                "musicbanner": [entry("https://assets.fanart.tv/fanart/music/a/banner.jpg",
                                      json!("0"))]
            }),
        ),
        (
            format!("/v3/music/albums/{artist}"),
            format!("/music/albums/{artist}"),
            json!({
                "name": "Seaside Radio",
                "mbid_id": artist,
                "albums": { album: {
                    "albumcover": [entry("https://assets.fanart.tv/fanart/music/a/cover.jpg",
                                         json!("5"))]
                } }
            }),
        ),
        (
            "/v3/movies/603".to_string(),
            "/movies/603".to_string(),
            json!({
                "name": "The Matrix",
                "tmdb_id": "603",
                "movieposter": [entry("https://assets.fanart.tv/fanart/movies/603/poster.jpg",
                                      json!("9"))],
                "moviebackground": [entry("https://assets.fanart.tv/fanart/movies/603/bg.jpg",
                                          json!("2"))],
                "moviebanner": [entry("https://assets.fanart.tv/fanart/movies/603/banner.jpg",
                                      json!(1))]
            }),
        ),
        (
            "/v3/tv/81797".to_string(),
            "/tv/81797".to_string(),
            json!({
                "name": "One Piece",
                "thetvdb_id": "81797",
                "tvposter": [entry("https://assets.fanart.tv/fanart/tv/81797/poster.jpg",
                                   json!("4"))],
                "showbackground": [entry("https://assets.fanart.tv/fanart/tv/81797/bg.jpg",
                                         json!("6"))],
                "tvbanner": [entry("https://assets.fanart.tv/fanart/tv/81797/banner.jpg",
                                   json!("8"))]
            }),
        ),
    ];

    for (route, upstream, golden) in cases {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(upstream.as_str()))
            .and(query_param("api_key", "server-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(golden.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let st = state(&["--fanart-api-key", "server-key", "--fanart-api-base", &server.uri()]);
        let (status, body) = get(&st, &route).await;
        assert_eq!(status, StatusCode::OK, "{route}");
        assert_eq!(body, golden, "{route}: without a caache base the payload is bit-for-bit");
    }
}

#[tokio::test]
async fn fanart_accepts_and_ignores_both_client_key_transports() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/music/{ARTIST_MBID}")))
        .and(query_param("api_key", "server-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "name": "Seaside Radio" })))
        .expect(3)
        .mount(&server)
        .await;

    let st = state(&["--fanart-api-key", "server-key", "--fanart-api-base", &server.uri()]);

    // Downstream sends the key as an `api-key` header at runtime but as
    // `?api_key=` in its connection probe; both must behave like a bare request.
    let bare = send(&st, "GET", &format!("/v3/music/{ARTIST_MBID}")).await;
    assert_eq!(bare.status(), StatusCode::OK);

    let query = send(&st, "GET", &format!("/v3/music/{ARTIST_MBID}?api_key=client-key")).await;
    assert_eq!(query.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri(format!("/v3/music/{ARTIST_MBID}"))
        .header("api-key", "client-key")
        .body(Body::empty())
        .unwrap();
    let header = build_router(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(header.status(), StatusCode::OK);

    for req in server.received_requests().await.unwrap() {
        assert_eq!(
            req.url.query_pairs().find(|(k, _)| k == "api_key").map(|(_, v)| v.to_string()),
            Some("server-key".to_string()),
            "upstream must only ever see the server-side key"
        );
    }
}

#[tokio::test]
async fn tmdb_search_results_always_carry_the_scored_fields() {
    let kinds = [
        ("movie", "title", "original_title", "release_date"),
        ("tv", "name", "original_name", "first_air_date"),
    ];
    for (kind, name_key, orig_key, date_key) in kinds {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/search/{kind}")))
            .and(query_param("query", "sparse"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "page": 1,
                "results": [{ "id": 42, name_key: "Sparse Result", "popularity": 1.5 }],
                "total_pages": 1, "total_results": 1
            })))
            .expect(1)
            .mount(&server)
            .await;

        let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
        let (status, body) = get(&st, &format!("/3/search/{kind}?query=sparse")).await;
        assert_eq!(status, StatusCode::OK, "{kind}");
        let result = &body["results"][0];
        assert_eq!(result["id"], 42, "{kind}");
        assert_eq!(result[orig_key], "Sparse Result", "{kind}: original_* falls back to title");
        assert_eq!(result[date_key], "", "{kind}: absent date serializes as empty string");
        assert_eq!(result["overview"], "", "{kind}: overview always present");
    }
}

#[tokio::test]
async fn tmdb_movie_detail_honours_append_union_and_exposes_certification() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/603"))
        .and(query_param("append_to_response", "external_ids,release_dates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 603,
            "title": "The Matrix",
            "original_title": "The Matrix",
            "release_date": "1999-03-30",
            "runtime": 136,
            "overview": "hacker",
            "imdb_id": "tt0133093",
            "genres": [{ "id": 28, "name": "Action" }],
            "production_companies": [{ "id": 79, "name": "Village Roadshow Pictures" }],
            "status": "Released",
            "vote_average": 8.2,
            "vote_count": 24000,
            "external_ids": { "imdb_id": "tt0133093" },
            "release_dates": { "results": [{
                "iso_3166_1": "US",
                "release_dates": [{ "certification": "R", "type": 3 }]
            }] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) =
        get(&st, "/3/movie/603?append_to_response=external_ids,release_dates").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "The Matrix");
    assert_eq!(body["runtime"], 136);
    assert_eq!(body["imdb_id"], "tt0133093");
    assert_eq!(body["external_ids"]["imdb_id"], "tt0133093");
    assert_eq!(body["genres"][0]["name"], "Action");
    let us = &body["release_dates"]["results"][0];
    assert_eq!(us["iso_3166_1"], "US");
    assert_eq!(us["release_dates"][0]["certification"], "R", "certification must be reachable");
}

#[tokio::test]
async fn tmdb_tv_detail_appends_content_ratings_and_lists_seasons() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/tv/1396"))
        .and(query_param("append_to_response", "content_ratings,external_ids"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 1396,
            "name": "Breaking Bad",
            "first_air_date": "2008-01-20",
            "overview": "chemistry",
            "seasons": [
                { "season_number": 0, "name": "Specials" },
                { "season_number": 1, "name": "Season 1" }
            ],
            "genres": [{ "id": 18, "name": "Drama" }],
            "networks": [{ "id": 174, "name": "AMC" }],
            "status": "Ended",
            "episode_run_time": [47],
            "vote_average": 8.9,
            "vote_count": 12000,
            "content_ratings": { "results": [{ "iso_3166_1": "US", "rating": "TV-MA" }] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/3/tv/1396?append_to_response=content_ratings").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Breaking Bad");
    assert_eq!(body["seasons"][0]["season_number"], 0);
    assert_eq!(body["seasons"][1]["name"], "Season 1");
    assert_eq!(body["episode_run_time"][0], 47);
    let rating = &body["content_ratings"]["results"][0];
    assert_eq!(rating["iso_3166_1"], "US");
    assert_eq!(rating["rating"], "TV-MA");
}

#[tokio::test]
async fn tmdb_season_lists_episode_fields_including_season_zero() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/tv/1396/season/0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "season_number": 0,
            "episodes": [{
                "episode_number": 1, "name": "Good Cop / Bad Cop",
                "runtime": 5, "overview": "minisode"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tmdb-api-key", "test-key", "--tmdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/3/tv/1396/season/0").await;
    assert_eq!(status, StatusCode::OK);
    let ep = &body["episodes"][0];
    assert_eq!(ep["episode_number"], 1);
    assert_eq!(ep["name"], "Good Cop / Bad Cop");
    assert_eq!(ep["runtime"], 5);
    assert_eq!(ep["overview"], "minisode");
}

#[tokio::test]
async fn tmdb_configuration_carries_the_preferred_image_sizes() {
    let st = state(&[]);
    let (status, body) = get(&st, "/3/configuration").await;
    assert_eq!(status, StatusCode::OK);
    let images = &body["images"];
    assert_eq!(images["secure_base_url"], "https://image.tmdb.org/t/p/");
    let contains =
        |key: &str, size: &str| images[key].as_array().is_some_and(|a| a.iter().any(|v| v == size));
    assert!(contains("poster_sizes", "w500"), "downstream's preferred poster size");
    assert!(contains("backdrop_sizes", "w1280"), "downstream's preferred backdrop size");
}

fn tvdb_login_mock() -> Mock {
    Mock::given(method("POST")).and(path("/login")).respond_with(
        ResponseTemplate::new(200).set_body_json(json!({ "data": { "token": "jwt-test" } })),
    )
}

#[tokio::test]
async fn tvdb_login_mints_the_token_shape_for_json_credential_bodies() {
    let st = state(&["--tvdb-api-key", "server-key"]);
    for body in [json!({ "apikey": "client-key" }), json!({ "apikey": "client-key", "pin": "42" })]
    {
        let resp = send_json(&st, "POST", "/v4/login", &body).await;
        assert_eq!(resp.status(), StatusCode::OK, "body {body}");
        let payload = body_json(resp).await;
        assert!(payload["data"]["token"].is_string(), "login must answer {{data:{{token}}}}");
    }
}

#[tokio::test]
async fn tvdb_search_entries_carry_id_aliases_and_translations() {
    let server = MockServer::start().await;
    tvdb_login_mock().mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/search"))
        .and(query_param("type", "series"))
        .and(query_param("query", "one piece"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "tvdb_id": "series-81797",
                "name": "One Piece",
                "year": "1999",
                "overview": "pirates",
                "aliases": ["ワンピース", "OP"],
                "translations": { "jpn": "ワンピース", "eng": "One Piece" }
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tvdb-api-key", "server-key", "--tvdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/v4/search?type=series&query=one%20piece").await;
    assert_eq!(status, StatusCode::OK);
    let hit = &body["data"][0];
    assert_eq!(hit["tvdb_id"], "series-81797", "prefixed slug id form must survive");
    assert_eq!(hit["name"], "One Piece");
    assert_eq!(hit["year"], "1999");
    assert_eq!(hit["aliases"][0], "ワンピース");
    assert_eq!(hit["translations"]["jpn"], "ワンピース");
}

#[tokio::test]
async fn tvdb_extended_exposes_official_seasons_and_typed_scored_artworks() {
    let server = MockServer::start().await;
    tvdb_login_mock().mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/series/81797/extended"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": {
                "id": 81797,
                "name": "One Piece",
                "firstAired": "1999-10-20",
                "overview": "pirates",
                "seasons": [
                    { "id": 1, "number": 1, "name": "Season 1",
                      "type": { "id": 1, "type": "official", "name": "Aired Order" } },
                    { "id": 2, "number": 1, "name": "Season 1 (DVD)",
                      "type": { "id": 2, "type": "dvd", "name": "DVD Order" } }
                ],
                "genres": [{ "id": 1, "name": "Animation" }],
                "status": { "name": "Continuing" },
                "originalNetwork": { "name": "Fuji TV" },
                "latestNetwork": { "name": "Fuji TV" },
                "averageRuntime": 25,
                "contentRatings": [{ "name": "TV-14", "country": "usa" }],
                "artworks": [
                    { "id": 10, "type": 1, "score": 100_005,
                      "image": "https://artworks.thetvdb.com/banners/graphical/b.jpg" },
                    { "id": 11, "type": "3", "score": 99_998,
                      "image": "https://artworks.thetvdb.com/banners/fanart/f.jpg" }
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tvdb-api-key", "server-key", "--tvdb-api-base", &server.uri()]);
    let (status, body) = get(&st, "/v4/series/81797/extended").await;
    assert_eq!(status, StatusCode::OK);
    let data = &body["data"];
    assert_eq!(data["name"], "One Piece");
    assert_eq!(data["firstAired"], "1999-10-20");
    assert_eq!(data["averageRuntime"], 25);
    assert_eq!(data["status"]["name"], "Continuing");
    assert_eq!(data["contentRatings"][0]["country"], "usa");
    assert_eq!(
        data["seasons"][0]["type"]["type"], "official",
        "type.type selects the aired order downstream"
    );
    assert_eq!(data["seasons"][0]["number"], 1);
    assert_eq!(data["seasons"][1]["type"]["type"], "dvd");
    // Artwork type ids 1 (banner) and 3 (background) are consumed in both int
    // and numeric-string form, with score deciding best-of.
    let artworks = data["artworks"].as_array().unwrap();
    assert_eq!(artworks[0]["type"], 1);
    assert_eq!(artworks[0]["score"], 100_005);
    assert_eq!(artworks[1]["type"], "3");
    assert_eq!(artworks[1]["score"], 99_998);
}

#[tokio::test]
async fn tvdb_episode_pages_signal_continuation_only_via_links_next() {
    let server = MockServer::start().await;
    tvdb_login_mock().mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/series/81797/episodes/official"))
        .and(query_param("season", "1"))
        .and(query_param("page", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": { "episodes": [{
                "id": 1, "number": 1, "seasonNumber": 1,
                "name": "I'm Luffy!", "runtime": 25, "overview": "rubber"
            }] },
            "links": { "prev": null, "next": "/series/81797/episodes/official?page=1" }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/series/81797/episodes/official"))
        .and(query_param("season", "1"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "data": { "episodes": [{ "id": 2, "number": 2, "seasonNumber": 1,
                                     "name": "Zoro", "runtime": 25, "overview": "swords" }] },
            "links": { "prev": "/series/81797/episodes/official?page=0", "next": null }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let st = state(&["--tvdb-api-key", "server-key", "--tvdb-api-base", &server.uri()]);

    let (status, body) = get(&st, "/v4/series/81797/episodes/official?season=1&page=0").await;
    assert_eq!(status, StatusCode::OK);
    let ep = &body["data"]["episodes"][0];
    assert_eq!(ep["number"], 1);
    assert_eq!(ep["seasonNumber"], 1);
    assert_eq!(ep["name"], "I'm Luffy!");
    assert_eq!(ep["runtime"], 25);
    assert_eq!(ep["overview"], "rubber");
    assert!(!body["links"]["next"].is_null(), "mid-page must advertise a next page");

    let (status, body) = get(&st, "/v4/series/81797/episodes/official?season=1&page=1").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["links"].get("next").is_some_and(Value::is_null),
        "last page must carry links.next = null — the walk's only stop signal"
    );
}
