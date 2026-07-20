//! `/cover` namespace — a universal artwork proxy cache (SHIB-32).
//!
//! Covers get their own namespace, decoupled from the metadata category roots.
//! Shirabe is a proxy cache over ALL artwork sources (Cover Art Archive, fanart.tv,
//! Wikimedia via MusicBrainz url-rels), not a per-provider facade: it owns source
//! selection, fallback, and caching, and hands the consumer ONE URL that returns
//! image bytes.
//!
//! - `/cover/artist/{mbid}` — fanart.tv artist images (prefer `artistthumb`, then
//!   `artistbackground`), falling back to the MusicBrainz `image` url-relation
//!   (Wikimedia file pages mapped to `Special:FilePath`).
//! - `/cover/release/{mbid}` (and `/release/{mbid}/{spec}`, spec ∈ [`CAA_SPECS`],
//!   default `front-500`) — Cover Art Archive, fanart album art
//!   (`/v3/music/albums/{mbid}` → `albumcover`) as a front-cover fallback.
//! - `/cover/tv/{id}` — TheTVDB series `image`, fanart.tv `/v3/tv/{id}`
//!   (`tvposter` / `clearart` / `showbackground`) as fallback.
//! - `/cover/movie/{id}` — TMDB `poster_path`, fanart.tv `/v3/movies/{id}`
//!   (`movieposter` / `moviebackground`) as fallback.
//!
//! Both the chosen upstream URL (per entity) and the fetched bytes are cached on
//! disk through [`crate::facades::coverart::CoverArtState`]; misses are
//! negative-cached so artless entities do not re-trigger upstream calls. Hits carry
//! a `Content-Type` and a long-lived `Cache-Control`; definitive misses are 404 so
//! consumers can render a placeholder.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::Value;
use uuid::Uuid;

use crate::facades::fanart;
use crate::{AppState, repo};

/// Outcome of resolving an entity to an upstream artwork URL.
enum Resolution {
    /// A chosen upstream image URL to fetch bytes from.
    Found(String),
    /// Sources were reachable but hold no art — negative-cache and 404.
    Artless,
    /// Sources could not be consulted (unconfigured / upstream / DB error) — 5xx,
    /// do NOT negative-cache.
    Unavailable,
}

/// Build the `/cover` route group, nested at `/cover` in `build_router`.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/artist/{mbid}", get(artist))
        .route("/release/{mbid}", get(release))
        .route("/release/{mbid}/{spec}", get(release_spec))
        .route("/tv/{id}", get(tv))
        .route("/movie/{id}", get(movie))
}

/// Allowed Cover Art Archive size/side specs. `front-500` is the default when the
/// bare `/release/{mbid}` route is hit.
const CAA_SPECS: &[&str] = &["front", "front-250", "front-500", "front-1200", "back"];

async fn artist(State(state): State<Arc<AppState>>, Path(mbid): Path<String>) -> Response {
    let Ok(gid) = Uuid::parse_str(&mbid) else {
        return (StatusCode::BAD_REQUEST, "invalid artist mbid").into_response();
    };
    let key = format!("cover:artist:{mbid}");
    if let Some(cached) = state.coverart.resolution_get(&key).await {
        return respond_cached(&state, cached).await;
    }
    let resolution = resolve_artist(&state, &mbid, gid).await;
    finish(&state, &key, resolution).await
}

async fn release(State(state): State<Arc<AppState>>, Path(mbid): Path<String>) -> Response {
    release_inner(&state, &mbid, "front-500").await
}

async fn release_spec(
    State(state): State<Arc<AppState>>,
    Path((mbid, spec)): Path<(String, String)>,
) -> Response {
    if !CAA_SPECS.contains(&spec.as_str()) {
        return (StatusCode::BAD_REQUEST, "invalid cover spec").into_response();
    }
    release_inner(&state, &mbid, &spec).await
}

async fn release_inner(state: &Arc<AppState>, mbid: &str, spec: &str) -> Response {
    if Uuid::parse_str(mbid).is_err() {
        return (StatusCode::BAD_REQUEST, "invalid release mbid").into_response();
    }
    let key = format!("cover:release:{mbid}:{spec}");
    if let Some(cached) = state.coverart.resolution_get(&key).await {
        return respond_cached(state, cached).await;
    }
    let resolution = resolve_release(state, mbid, spec).await;
    finish(state, &key, resolution).await
}

async fn tv(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let key = format!("cover:tv:{id}");
    if let Some(cached) = state.coverart.resolution_get(&key).await {
        return respond_cached(&state, cached).await;
    }
    let resolution = resolve_tv(&state, &id).await;
    finish(&state, &key, resolution).await
}

async fn movie(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let key = format!("cover:movie:{id}");
    if let Some(cached) = state.coverart.resolution_get(&key).await {
        return respond_cached(&state, cached).await;
    }
    let resolution = resolve_movie(&state, &id).await;
    finish(&state, &key, resolution).await
}

/// Serve a previously-resolved entity from cache: fetch bytes for the chosen URL,
/// or 404 for a cached negative.
async fn respond_cached(state: &Arc<AppState>, cached: Option<String>) -> Response {
    let Some(url) = cached else {
        return not_found(state);
    };
    match state.coverart.fetch_cover_bytes(&url).await {
        (StatusCode::OK, ct, body) => image_ok(state, ct, body),
        (StatusCode::NOT_FOUND, _, _) => not_found(state),
        _ => bad_gateway(),
    }
}

/// Fetch bytes for a freshly-resolved entity and persist the resolution outcome.
async fn finish(state: &Arc<AppState>, key: &str, resolution: Resolution) -> Response {
    match resolution {
        Resolution::Found(url) => match state.coverart.fetch_cover_bytes(&url).await {
            (StatusCode::OK, ct, body) => {
                state.coverart.resolution_put(key, Some(&url)).await;
                image_ok(state, ct, body)
            }
            (StatusCode::NOT_FOUND, _, _) => {
                state.coverart.resolution_put(key, None).await;
                not_found(state)
            }
            _ => bad_gateway(),
        },
        Resolution::Artless => {
            state.coverart.resolution_put(key, None).await;
            not_found(state)
        }
        Resolution::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "cover source unavailable").into_response()
        }
    }
}

async fn resolve_artist(state: &Arc<AppState>, mbid: &str, gid: Uuid) -> Resolution {
    if let Some(payload) = fanart::fetch_raw(state, mbid, "music", &format!("music/{mbid}")).await
        && let Some(url) =
            first_url(&payload, "artistthumb").or_else(|| first_url(&payload, "artistbackground"))
    {
        return Resolution::Found(url);
    }
    match musicbrainz_artist_image(state, gid).await {
        Ok(Some(url)) => Resolution::Found(url),
        Ok(None) => Resolution::Artless,
        Err(()) => Resolution::Unavailable,
    }
}

async fn resolve_release(state: &Arc<AppState>, mbid: &str, spec: &str) -> Resolution {
    let caa = format!("{}/release/{mbid}/{spec}", state.coverart.upstream_base());
    match state.coverart.fetch_cover_bytes(&caa).await.0 {
        StatusCode::OK => return Resolution::Found(caa),
        StatusCode::NOT_FOUND => {}
        _ => return Resolution::Unavailable,
    }
    // fanart album art is a front-cover fallback only; side/back specs have none.
    if spec.starts_with("front")
        && let Some(payload) =
            fanart::fetch_raw(state, mbid, "music_albums", &format!("music/albums/{mbid}")).await
    {
        return album_cover_url(&payload, mbid).map_or(Resolution::Artless, Resolution::Found);
    }
    Resolution::Artless
}

async fn resolve_tv(state: &Arc<AppState>, id: &str) -> Resolution {
    if let Some(url) = crate::facades::tvdb::series_image_url(state, id).await {
        return Resolution::Found(url);
    }
    let Some(payload) = fanart::fetch_raw(state, id, "tv", &format!("tv/{id}")).await else {
        return Resolution::Unavailable;
    };
    first_url(&payload, "tvposter")
        .or_else(|| first_url(&payload, "clearart"))
        .or_else(|| first_url(&payload, "showbackground"))
        .map_or(Resolution::Artless, Resolution::Found)
}

async fn resolve_movie(state: &Arc<AppState>, id: &str) -> Resolution {
    if let Some(url) = crate::facades::tmdb::movie_poster_url(state, id).await {
        return Resolution::Found(url);
    }
    let Some(payload) = fanart::fetch_raw(state, id, "movies", &format!("movies/{id}")).await
    else {
        return Resolution::Unavailable;
    };
    first_url(&payload, "movieposter")
        .or_else(|| first_url(&payload, "moviebackground"))
        .map_or(Resolution::Artless, Resolution::Found)
}

/// The MusicBrainz `image` url-relation for an artist, mapping Wikimedia file pages
/// to `Special:FilePath`. `Ok(None)` = reachable but no relation; `Err` = DB error.
async fn musicbrainz_artist_image(state: &Arc<AppState>, gid: Uuid) -> Result<Option<String>, ()> {
    let lookup = repo::lookup_artist(state.pool(), gid, true).await.map_err(|e| {
        tracing::warn!(error = %e, "cover artist url-rels lookup failed");
    })?;
    let Some(lookup) = lookup else {
        return Ok(None);
    };
    Ok(lookup
        .relations
        .into_iter()
        .find(|r| r.rel_type == "image")
        .map(|r| wikimedia_direct_url(&r.url.resource)))
}

/// First `url` in the artwork array under `key` (fanart.tv shape).
fn first_url(payload: &Value, key: &str) -> Option<String> {
    payload.get(key)?.as_array()?.first()?.get("url")?.as_str().map(ToString::to_string)
}

/// First album-cover URL from a fanart.tv `music/albums` payload, keyed by the
/// requested `mbid` (falling back to the first album entry).
fn album_cover_url(payload: &Value, mbid: &str) -> Option<String> {
    let albums = payload.get("albums")?.as_object()?;
    let entry = albums.get(mbid).or_else(|| albums.values().next())?;
    first_url(entry, "albumcover").or_else(|| first_url(entry, "cdart"))
}

/// Map a Wikimedia Commons file-description page to a direct image URL via
/// `Special:FilePath` (which 302-redirects to the file). Non-Commons URLs are
/// returned unchanged.
fn wikimedia_direct_url(resource: &str) -> String {
    if let Some(idx) = resource.find("/wiki/File:") {
        let host = &resource[..idx];
        let file = &resource[idx + "/wiki/File:".len()..];
        return format!("{host}/wiki/Special:FilePath/{file}");
    }
    resource.to_string()
}

fn image_ok(state: &Arc<AppState>, content_type: Option<String>, body: Vec<u8>) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    if let Some(ct) = content_type
        && let Ok(value) = HeaderValue::from_str(&ct)
    {
        resp.headers_mut().insert(CONTENT_TYPE, value);
    }
    set_cache_control(&mut resp, state.config.coverart_positive_ttl_secs);
    resp
}

fn not_found(state: &Arc<AppState>) -> Response {
    let mut resp = (StatusCode::NOT_FOUND, "no cover").into_response();
    set_cache_control(&mut resp, state.config.coverart_negative_ttl_secs);
    resp
}

fn bad_gateway() -> Response {
    (StatusCode::BAD_GATEWAY, "cover upstream error").into_response()
}

fn set_cache_control(resp: &mut Response, max_age_secs: u64) {
    if let Ok(value) = HeaderValue::from_str(&format!("public, max-age={max_age_secs}")) {
        resp.headers_mut().insert(CACHE_CONTROL, value);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn artist_prefers_thumb_then_background() {
        let payload = json!({
            "artistthumb": [{ "url": "https://assets.fanart.tv/a/thumb.jpg" }],
            "artistbackground": [{ "url": "https://assets.fanart.tv/a/bg.jpg" }],
        });
        assert_eq!(
            first_url(&payload, "artistthumb").or_else(|| first_url(&payload, "artistbackground")),
            Some("https://assets.fanart.tv/a/thumb.jpg".to_string())
        );

        let bg_only =
            json!({ "artistbackground": [{ "url": "https://assets.fanart.tv/a/bg.jpg" }] });
        assert_eq!(
            first_url(&bg_only, "artistthumb").or_else(|| first_url(&bg_only, "artistbackground")),
            Some("https://assets.fanart.tv/a/bg.jpg".to_string())
        );

        let empty = json!({ "name": "x" });
        assert_eq!(first_url(&empty, "artistthumb"), None);
    }

    #[test]
    fn tv_prefers_poster_then_clearart_then_background() {
        let full = json!({
            "tvposter": [{ "url": "https://assets.fanart.tv/tv/poster.jpg" }],
            "clearart": [{ "url": "https://assets.fanart.tv/tv/clear.png" }],
            "showbackground": [{ "url": "https://assets.fanart.tv/tv/bg.jpg" }],
        });
        assert_eq!(
            first_url(&full, "tvposter")
                .or_else(|| first_url(&full, "clearart"))
                .or_else(|| first_url(&full, "showbackground")),
            Some("https://assets.fanart.tv/tv/poster.jpg".to_string())
        );

        let no_poster =
            json!({ "showbackground": [{ "url": "https://assets.fanart.tv/tv/bg.jpg" }] });
        assert_eq!(
            first_url(&no_poster, "tvposter")
                .or_else(|| first_url(&no_poster, "clearart"))
                .or_else(|| first_url(&no_poster, "showbackground")),
            Some("https://assets.fanart.tv/tv/bg.jpg".to_string())
        );
    }

    #[test]
    fn movie_prefers_poster_then_background() {
        let payload = json!({
            "movieposter": [{ "url": "https://assets.fanart.tv/m/poster.jpg" }],
            "moviebackground": [{ "url": "https://assets.fanart.tv/m/bg.jpg" }],
        });
        assert_eq!(
            first_url(&payload, "movieposter").or_else(|| first_url(&payload, "moviebackground")),
            Some("https://assets.fanart.tv/m/poster.jpg".to_string())
        );
    }

    #[test]
    fn album_cover_picks_by_mbid_then_falls_back() {
        let mbid = "76df3287-6cda-33eb-8e9a-044b5e15ffdd";
        let payload = json!({
            "albums": {
                "76df3287-6cda-33eb-8e9a-044b5e15ffdd": {
                    "albumcover": [{ "url": "https://assets.fanart.tv/al/cover.jpg" }],
                    "cdart": [{ "url": "https://assets.fanart.tv/al/cd.png" }]
                }
            }
        });
        assert_eq!(
            album_cover_url(&payload, mbid),
            Some("https://assets.fanart.tv/al/cover.jpg".to_string())
        );

        // Unknown mbid falls back to the first album entry.
        assert_eq!(
            album_cover_url(&payload, "00000000-0000-0000-0000-000000000000"),
            Some("https://assets.fanart.tv/al/cover.jpg".to_string())
        );

        let cd_only = json!({ "albums": { "x": { "cdart": [{ "url": "https://a/cd.png" }] } } });
        assert_eq!(album_cover_url(&cd_only, "x"), Some("https://a/cd.png".to_string()));

        assert_eq!(album_cover_url(&json!({ "albums": {} }), "x"), None);
    }

    #[test]
    fn commons_file_page_maps_to_filepath() {
        assert_eq!(
            wikimedia_direct_url("https://commons.wikimedia.org/wiki/File:Nirvana_1992.jpg"),
            "https://commons.wikimedia.org/wiki/Special:FilePath/Nirvana_1992.jpg"
        );
        let direct = "https://assets.fanart.tv/fanart/music/abc/artistthumb/x.jpg";
        assert_eq!(wikimedia_direct_url(direct), direct);
    }

    #[tokio::test]
    async fn resolution_cache_round_trips_positive_and_negative() {
        use crate::config::Cli;
        use crate::facades::coverart::CoverArtState;
        use clap::Parser;

        let dir = std::env::temp_dir().join(format!(
            "shirabe-cover-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let cli = Cli::try_parse_from([
            "shirabe",
            "--database-url",
            "postgres://x/x",
            "--coverart-cache-dir",
            dir.to_str().unwrap(),
        ])
        .unwrap();
        let ca = CoverArtState::new(&cli.config);

        assert!(ca.resolution_get("cover:artist:a").await.is_none());

        ca.resolution_put("cover:artist:a", Some("https://img/a.jpg")).await;
        assert_eq!(
            ca.resolution_get("cover:artist:a").await,
            Some(Some("https://img/a.jpg".to_string()))
        );

        ca.resolution_put("cover:artist:b", None).await;
        assert_eq!(ca.resolution_get("cover:artist:b").await, Some(None));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
