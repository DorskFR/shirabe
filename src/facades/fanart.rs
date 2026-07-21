//! fanart.tv v3 facade.
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ ATTRIBUTION — fanart.tv                                                     │
//! │                                                                            │
//! │ Artwork provided by fanart.tv (https://fanart.tv). This deployment uses a  │
//! │ single project API key (+ optional personal `client_key`) held strictly    │
//! │ server-side (`FANART_API_KEY` / `FANART_PERSONAL_API_KEY`); the real key is │
//! │ NEVER re-exposed to clients.                                                │
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! Mounts the `/v3/*` endpoints Kusaritoi's fanart.tv provider calls, mirroring the
//! upstream v3 JSON shapes. Each handler is cache-first: it serves a fresh row from
//! `fanart_cache` (in the dedicated `fanart` DB; TTL = `FANART_CACHE_TTL_DAYS`,
//! default 7d) when present, otherwise calls the v3 API once with the held key,
//! stores the payload, and returns it. A second identical call is served from cache
//! and never hits upstream.
//!
//! Graceful degradation: when `FANART_API_KEY` is unset, a request that would need
//! upstream returns a clean 503 in fanart.tv's `{status:"error", error message}`
//! shape — never a panic — while cached rows are still served. The API server still
//! boots and serves `/ws/2` + the other facades.
//!
//! Asset URLs (`assets.fanart.tv/...`) in responses are rewritten to route through
//! the caache byte proxy (`/_ia/<host>/<path>`) so large images are fetched + cached
//! there rather than streamed straight to the timeout-bound client.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::{AppState, images};

/// Upstream fanart.tv v3 API base.
const API_BASE: &str = "https://webservice.fanart.tv/v3";

/// fanart.tv JSON keys whose values are ABSOLUTE image URLs (on `assets.fanart.tv`).
/// Every artwork entry exposes its bytes under a `url` field, with an optional
/// smaller `preview`. When a caache base is configured each is rewritten to route
/// through the caache `/_ia/<host>/<path>` proxy. Applied recursively to the whole
/// payload (nested artwork arrays: `artistthumb`, `musiclogo`, `albums.*`, …).
const FANART_IMAGE_URL_KEYS: &[&str] = &["url", "preview"];

/// Recursively rewrite fanart.tv absolute image-URL fields in `value` to route
/// through the caache proxy. A None/empty base disables rewriting (no-op). Only
/// absolute http(s) values are rewritten. Stateless: only URL strings change.
fn rewrite_image_urls(base: Option<&str>, value: &mut Value) {
    let Some(base) = base.filter(|b| !b.is_empty()) else {
        return;
    };
    rewrite_image_urls_inner(base, value);
}

fn rewrite_image_urls_inner(base: &str, value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if FANART_IMAGE_URL_KEYS.contains(&k.as_str())
                    && let Some(url) = v.as_str()
                {
                    *v = Value::String(images::rewrite_through_caache(base, url));
                    continue;
                }
                rewrite_image_urls_inner(base, v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_image_urls_inner(base, v);
            }
        }
        _ => {}
    }
}

/// Build the `/v3` route group.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v3/music/{mbid}", get(music))
        .route("/v3/music/albums/{mbid}", get(music_albums))
        .route("/v3/movies/{id}", get(movies))
        .route("/v3/tv/{id}", get(tv))
}

/// Is a cache row fresh? `age_secs` is `now - fetched_at`; rows at/under the TTL
/// (in days) are served, older rows are re-fetched. A non-positive TTL disables
/// caching (always stale). The live freshness test runs in SQL; this pure mirror
/// documents and unit-tests the TTL semantics.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
const fn is_fresh(age_secs: i64, ttl_days: i64) -> bool {
    if ttl_days <= 0 {
        return false;
    }
    age_secs >= 0 && age_secs <= ttl_days * 86_400
}

/// fanart.tv-shaped error body + status. Used when the key is absent or upstream
/// fails. Shape: `{ "status": "error", "error message": "…" }`.
fn fanart_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "status": "error", "error message": message }))).into_response()
}

/// The 503 returned when no server-side key is configured and the request can't be
/// served from cache.
fn not_configured() -> Response {
    fanart_error(StatusCode::SERVICE_UNAVAILABLE, "fanart.tv source not configured")
}

/// The writable `fanart` cache pool, or `None` when `FANART_DATABASE_URL` is unset.
const fn fanart_pool(state: &AppState) -> Option<&PgPool> {
    state.pools.fanart.as_ref()
}

/// Fetch a fresh cached payload for `(key, kind)`, honouring the configured TTL.
/// The freshness test is done in SQL (`fetched_at` vs `now()`). Returns `None` on
/// miss / stale / no pool.
async fn cache_get(state: &AppState, key: &str, kind: &str) -> Option<Value> {
    let pool = fanart_pool(state)?;
    let ttl_days = state.config.fanart_cache_ttl_days;
    if ttl_days <= 0 {
        return None;
    }
    let row = sqlx::query(
        "SELECT payload FROM fanart_cache
         WHERE cache_key = $1 AND kind = $2
           AND fetched_at >= now() - ($3 || ' days')::interval",
    )
    .bind(key)
    .bind(kind)
    .bind(ttl_days.to_string())
    .fetch_optional(pool)
    .await
    .ok()??;
    row.try_get::<Value, _>("payload").ok()
}

/// Store (upsert) a payload into `fanart_cache` with `fetched_at = now()`.
/// Best-effort: a cache write failure is logged but does not fail the request.
async fn cache_put(state: &AppState, key: &str, kind: &str, payload: &Value) {
    let Some(pool) = fanart_pool(state) else {
        return;
    };
    let res = sqlx::query(
        "INSERT INTO fanart_cache (cache_key, kind, payload, fetched_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (cache_key, kind) DO UPDATE SET
             payload    = EXCLUDED.payload,
             fetched_at = EXCLUDED.fetched_at",
    )
    .bind(key)
    .bind(kind)
    .bind(payload)
    .execute(pool)
    .await;
    if let Err(e) = res {
        tracing::warn!(error = %e, kind, key, "fanart cache write failed");
    }
}

/// Perform an upstream fanart.tv v3 GET, returning the parsed JSON body. `path` is
/// the endpoint path under [`API_BASE`] (no leading slash). The held project
/// `api_key` (and optional personal `client_key`) are appended as query params.
async fn upstream_get(state: &AppState, path: &str) -> Result<Value, String> {
    let Some(api_key) = state.config.fanart_api_key.as_deref() else {
        return Err("fanart.tv api key not configured".to_string());
    };
    let client = reqwest::Client::builder()
        .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let mut query: Vec<(&str, String)> = vec![("api_key", api_key.to_string())];
    if let Some(client_key) = state.config.fanart_personal_api_key.as_deref() {
        query.push(("client_key", client_key.to_string()));
    }
    let url = format!("{API_BASE}/{path}");
    let resp = client
        .get(&url)
        .query(&query)
        .send()
        .await
        .map_err(|e| format!("upstream request: {e}"))?
        .error_for_status()
        .map_err(|e| format!("upstream status: {e}"))?;
    let bytes = resp.bytes().await.map_err(|e| format!("upstream body: {e}"))?;
    serde_json::from_slice::<Value>(&bytes).map_err(|e| format!("upstream json: {e}"))
}

/// Shared cache-first handler: serve a fresh cached row for `(key, kind)`, else
/// fetch the upstream `path` once, cache it, and return it — always rewriting asset
/// URLs through caache. Degrades to a clean 503 when no key is configured and the
/// row is not cached.
async fn cached_fetch(state: &Arc<AppState>, key: &str, kind: &str, path: &str) -> Response {
    if let Some(mut cached) = cache_get(state, key, kind).await {
        rewrite_image_urls(state.config.caache_base_url.as_deref(), &mut cached);
        return Json(cached).into_response();
    }
    if state.config.fanart_api_key.is_none() {
        return not_configured();
    }
    match upstream_get(state, path).await {
        Ok(mut payload) => {
            cache_put(state, key, kind, &payload).await;
            rewrite_image_urls(state.config.caache_base_url.as_deref(), &mut payload);
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, kind, key, "fanart upstream failed");
            fanart_error(StatusCode::BAD_GATEWAY, "fanart.tv upstream error")
        }
    }
}

/// `GET /v3/music/{mbid}` → artist artwork (`artistthumb`, `artistbackground`,
/// `musiclogo`, `hdmusiclogo`, `musicbanner`, …).
async fn music(State(state): State<Arc<AppState>>, Path(mbid): Path<String>) -> Response {
    cached_fetch(&state, &mbid, "music", &format!("music/{mbid}")).await
}

/// `GET /v3/music/albums/{mbid}` → album artwork (`albumcover`, `cdart`) keyed by
/// the artist MBID.
async fn music_albums(State(state): State<Arc<AppState>>, Path(mbid): Path<String>) -> Response {
    cached_fetch(&state, &mbid, "music_albums", &format!("music/albums/{mbid}")).await
}

/// `GET /v3/movies/{id}` → movie artwork keyed by TMDB or IMDb id.
async fn movies(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    cached_fetch(&state, &id, "movies", &format!("movies/{id}")).await
}

/// `GET /v3/tv/{id}` → TV artwork keyed by TheTVDB id.
async fn tv(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    cached_fetch(&state, &id, "tv", &format!("tv/{id}")).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TTL staleness: rows within the window are fresh; older or future-skewed rows
    /// are not; a non-positive TTL disables caching.
    #[test]
    fn cache_ttl_freshness() {
        let week = 7 * 86_400;
        assert!(is_fresh(0, 7)); // just written
        assert!(is_fresh(week - 1, 7)); // within window
        assert!(is_fresh(week, 7)); // exactly at TTL boundary → fresh
        assert!(!is_fresh(week + 1, 7)); // past TTL → stale
        assert!(!is_fresh(-5, 7)); // clock skew (future) → stale
        assert!(!is_fresh(0, 0)); // TTL disabled
        assert!(!is_fresh(0, -1)); // negative TTL disabled
    }

    /// Absolute fanart.tv asset URLs nested in artwork arrays (and their `preview`
    /// thumbnails) are rewritten through caache; a None base is a no-op.
    #[test]
    fn rewrites_nested_asset_urls() {
        let base = "https://caache.dorsk.dev";
        let mut payload = json!({
            "name": "Radiohead",
            "mbid_id": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
            "artistthumb": [
                {
                    "id": "10714",
                    "url": "https://assets.fanart.tv/fanart/music/a74b/artistthumb/x.jpg",
                    "likes": "3"
                }
            ],
            "musiclogo": [
                {
                    "id": "1234",
                    "url": "https://assets.fanart.tv/fanart/music/a74b/musiclogo/y.png",
                    "preview": "http://assets.fanart.tv/preview/fanart/music/a74b/musiclogo/y.png"
                }
            ]
        });
        rewrite_image_urls(Some(base), &mut payload);
        assert_eq!(
            payload["artistthumb"][0]["url"],
            "https://caache.dorsk.dev/_ia/assets.fanart.tv/fanart/music/a74b/artistthumb/x.jpg"
        );
        assert_eq!(
            payload["musiclogo"][0]["url"],
            "https://caache.dorsk.dev/_ia/assets.fanart.tv/fanart/music/a74b/musiclogo/y.png"
        );
        assert_eq!(
            payload["musiclogo"][0]["preview"],
            "https://caache.dorsk.dev/_ia/assets.fanart.tv/preview/fanart/music/a74b/musiclogo/y.png"
        );
        // Non-URL fields untouched.
        assert_eq!(payload["name"], "Radiohead");
        assert_eq!(payload["artistthumb"][0]["likes"], "3");

        // None base disables rewriting.
        let mut original =
            json!({ "artistthumb": [{ "url": "https://assets.fanart.tv/fanart/music/a/x.jpg" }] });
        rewrite_image_urls(None, &mut original);
        assert_eq!(
            original["artistthumb"][0]["url"],
            "https://assets.fanart.tv/fanart/music/a/x.jpg"
        );
    }
}
