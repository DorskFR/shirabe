//! MB facade behaviour against a seeded musicbrainz-schema fixture DB: every
//! ws/2 route returns real 200 payloads in the MB hyphenated-key contract, plus
//! DB-backed 404s. #[ignore]d; run via `make test-integration`.

mod common;

use axum::http::StatusCode;
use common::{
    ABSENT_MBID, ARTIST_MBID, RECORDING_MBID, RELEASE_GROUP_MBID, RELEASE_MBID, get, mb_fixture_db,
    state_with_db,
};

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn ws2_search_endpoints_serve_fixture_data() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) = get(&st, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");

    let (status, body) = get(&st, "/ws/2/artist?query=seaside%20radio").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["offset"], 0);
    let artist = &body["artists"][0];
    assert_eq!(artist["id"], ARTIST_MBID);
    assert_eq!(artist["name"], "Seaside Radio");
    assert_eq!(artist["score"], 100);
    assert_eq!(artist["aliases"][0]["name"], "Régio Costera");
    assert_eq!(artist["aliases"][0]["sort-name"], "Costera, Régio");

    let (status, body) = get(&st, "/ws/2/artist?query=regio%20costera").await;
    assert_eq!(status, StatusCode::OK, "alias search, accent-folded");
    assert_eq!(body["artists"][0]["id"], ARTIST_MBID);

    let (status, body) = get(&st, "/ws/2/artist?query=seasid").await;
    assert_eq!(status, StatusCode::OK, "trigram fallback on partial query");
    assert_eq!(body["artists"][0]["id"], ARTIST_MBID);

    let (status, body) = get(&st, "/ws/2/release?query=harbour%20lights").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    let release = &body["releases"][0];
    assert_eq!(release["title"], "Harbour Lights");
    assert_eq!(release["status"], "Official");
    assert_eq!(release["artist-credit"][0]["artist"]["name"], "Seaside Radio");
    assert_eq!(release["release-group"]["id"], RELEASE_GROUP_MBID);
    let dates: Vec<&str> =
        body["releases"].as_array().unwrap().iter().map(|r| r["date"].as_str().unwrap()).collect();
    assert!(dates.contains(&"1997-05-21") && dates.contains(&"1998"), "dates: {dates:?}");

    let (status, body) =
        get(&st, &format!("/ws/2/release?query=arid:{ARTIST_MBID}%20AND%20status:official")).await;
    assert_eq!(status, StatusCode::OK, "arid browse path");
    assert_eq!(body["count"], 2);
    assert_eq!(body["releases"][0]["id"], RELEASE_MBID);
    assert_eq!(body["releases"][0]["track-count"], 2);

    let (status, body) = get(&st, "/music/recording?query=lighthouse%20keeper").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    let rec = &body["recordings"][0];
    assert_eq!(rec["id"], RECORDING_MBID);
    assert_eq!(rec["length"], 387_000);
    assert_eq!(rec["releases"][0]["media"][0]["track-count"], 2);
    assert_eq!(rec["releases"][0]["media"][0]["format"], "CD");
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn ws2_lookup_endpoints_serve_fixture_data() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) =
        get(&st, &format!("/ws/2/artist/{ARTIST_MBID}?inc=url-rels%2Bgenres%2Btags%2Bannotation"))
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Seaside Radio");
    assert_eq!(body["sort-name"], "Seaside Radio");
    assert_eq!(body["type"], "Group");
    assert_eq!(body["disambiguation"], "test band");
    assert_eq!(body["relations"][0]["type"], "official homepage");
    assert_eq!(body["relations"][0]["url"]["resource"], "https://seaside-radio.example");
    assert_eq!(body["genres"][0]["name"], "rock");
    assert_eq!(body["genres"][0]["count"], 5);
    assert_eq!(body["tags"][0]["name"], "rock");
    assert_eq!(body["annotation"], "seaside radio annotation");

    let (status, body) = get(&st, &format!("/musicbrainz/ws/2/release/{RELEASE_MBID}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "Harbour Lights");
    assert_eq!(body["date"], "1997-05-21");
    assert_eq!(body["disambiguation"], "deluxe edition");
    assert_eq!(body["track-count"], 2);
    let medium = &body["media"][0];
    assert_eq!(medium["format"], "CD");
    assert_eq!(medium["tracks"][0]["title"], "Foghorn Morning");
    assert_eq!(medium["tracks"][1]["recording"]["id"], RECORDING_MBID);
    assert_eq!(body["relations"][0]["direction"], "forward");

    let (status, body) = get(&st, &format!("/music/recording/{RECORDING_MBID}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "Lighthouse Keeper");
    assert_eq!(body["artist-credit"][0]["artist"]["aliases"][0]["name"], "Régio Costera");
    assert_eq!(body["releases"][0]["id"], RELEASE_MBID);

    let (status, body) = get(&st, &format!("/ws/2/release-group/{RELEASE_GROUP_MBID}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "Harbour Lights");
    assert_eq!(body["primary-type"], "Album");
    assert_eq!(body["secondary-types"][0], "Live");
    assert_eq!(body["first-release-date"], "1997-05-21");
    assert_eq!(body["releases"].as_array().unwrap().len(), 2);
    assert_eq!(body["releases"][0]["status"], "Official");
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn ws2_browse_and_db_backed_404s() {
    let db = mb_fixture_db().await;
    let st = state_with_db(&db, &[]);

    let (status, body) = get(&st, &format!("/ws/2/release-group?artist={ARTIST_MBID}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["release-group-count"], 1);
    assert_eq!(body["release-group-offset"], 0);
    let rg = &body["release-groups"][0];
    assert_eq!(rg["id"], RELEASE_GROUP_MBID);
    assert_eq!(rg["primary-type"], "Album");
    assert_eq!(rg["artist-credit"][0]["artist"]["id"], ARTIST_MBID);

    let (status, body) = get(&st, &format!("/ws/2/release-group?artist={ABSENT_MBID}")).await;
    assert_eq!(status, StatusCode::OK, "unknown artist browses empty, not 404");
    assert_eq!(body["release-group-count"], 0);

    for path in [
        format!("/ws/2/artist/{ABSENT_MBID}"),
        format!("/ws/2/release/{ABSENT_MBID}"),
        format!("/ws/2/recording/{ABSENT_MBID}"),
        format!("/ws/2/release-group/{ABSENT_MBID}"),
        format!("/music/artist/{ABSENT_MBID}"),
    ] {
        let (status, body) = get(&st, &path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(body["error"], "not found", "{path}");
    }

    let (status, body) = get(&st, "/ws/2/artist?query=zzzzqqqq").await;
    assert_eq!(status, StatusCode::OK, "no-match search is an empty 200");
    assert_eq!(body["count"], 0);
    assert_eq!(body["artists"], serde_json::json!([]));
}
