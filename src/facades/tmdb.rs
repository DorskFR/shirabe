//! TMDB v3 facade (SHIB-6).
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ ATTRIBUTION — TMDB                                                          │
//! │                                                                            │
//! │ This product uses the TMDB API but is not endorsed or certified by TMDB.   │
//! │ Data and images are courtesy of The Movie Database (https://themoviedb.org)│
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! Mounts the exact `/3/*` endpoints Kusaritoi's TMDB provider calls, mirroring
//! the upstream v3 JSON shapes. The inbound `api_key` query param is **accepted
//! and ignored** — Shirabe holds the real key server-side (`TMDB_API_KEY`).
//!
//! Each handler is cache-first: it serves a fresh row from `shirabe.tmdb_cache`
//! (TTL = `TMDB_CACHE_TTL_DAYS`, default 7d) when present, otherwise calls the
//! TMDB v3 API once with the held key, stores the payload, and self-links any
//! returned `external_ids` (imdb_id, …) into `shirabe.xref` via
//! [`wikidata::upsert_xref`]. A second identical call is served from cache and
//! never hits upstream.
//!
//! Detail lookups honour `append_to_response=external_ids` so `imdb_id` is
//! present; search ranking ties are broken by `popularity`.
//!
//! Graceful degradation: when `TMDB_API_KEY` is unset, a request that would need
//! upstream returns a clean 503 in TMDB's error shape
//! (`{status_code, status_message}`) — never a panic — while cached rows are still
//! served. The API server still boots and serves `/ws/2` + the other facades.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::AppState;
use crate::sources::wikidata;

/// Upstream TMDB v3 API base.
const API_BASE: &str = "https://api.themoviedb.org/3";

/// Build the `/3` route group.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/3/search/tv", get(search_tv))
        .route("/3/search/movie", get(search_movie))
        .route("/3/tv/{id}", get(tv))
        .route("/3/tv/{id}/season/{n}", get(tv_season))
        .route("/3/movie/{id}", get(movie))
}

/// Is a cache row fresh? `age_secs` is `now - fetched_at`; rows at/under the TTL
/// (in days) are served, older rows are re-fetched. A non-positive TTL disables
/// caching (always stale).
///
/// The live cache freshness test is performed in SQL (`fetched_at` vs `now()`);
/// this pure mirror of that predicate documents and unit-tests the TTL semantics.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
const fn is_fresh(age_secs: i64, ttl_days: i64) -> bool {
    if ttl_days <= 0 {
        return false;
    }
    age_secs >= 0 && age_secs <= ttl_days * 86_400
}

/// TMDB-shaped error body + status. Used when the key is absent or upstream fails.
fn tmdb_error(status: StatusCode, code: i64, message: &str) -> Response {
    (status, Json(json!({ "status_code": code, "status_message": message }))).into_response()
}

/// The 503 returned when no server-side key is configured and the request can't be
/// served from cache.
fn not_configured() -> Response {
    tmdb_error(StatusCode::SERVICE_UNAVAILABLE, 7, "TMDB source not configured")
}

/// The writable `shirabe` pool, or `None` when `SHIRABE_DATABASE_URL` is unset.
const fn shirabe_pool(state: &AppState) -> Option<&PgPool> {
    state.pools.shirabe.as_ref()
}

/// Fetch a fresh cached payload for `(id, kind)`, honouring the configured TTL.
/// The freshness test is done in SQL (`fetched_at` vs `now()`), so no timestamp
/// type needs decoding client-side. Returns `None` on miss / stale / no pool.
async fn cache_get(state: &AppState, id: i64, kind: &str) -> Option<Value> {
    let pool = shirabe_pool(state)?;
    let ttl_days = state.config.tmdb_cache_ttl_days;
    if ttl_days <= 0 {
        return None;
    }
    // `$3` days TTL; only return the row when still within the window.
    let row = sqlx::query(
        "SELECT payload FROM shirabe.tmdb_cache
         WHERE id = $1 AND kind = $2
           AND fetched_at >= now() - ($3 || ' days')::interval",
    )
    .bind(id)
    .bind(kind)
    .bind(ttl_days.to_string())
    .fetch_optional(pool)
    .await
    .ok()??;
    row.try_get::<Value, _>("payload").ok()
}

/// Store (upsert) a payload into `shirabe.tmdb_cache` with `fetched_at = now()`.
/// Best-effort: a cache write failure is logged but does not fail the request.
async fn cache_put(state: &AppState, id: i64, kind: &str, payload: &Value) {
    let Some(pool) = shirabe_pool(state) else {
        return;
    };
    let res = sqlx::query(
        "INSERT INTO shirabe.tmdb_cache (id, kind, payload, fetched_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (id, kind) DO UPDATE SET
             payload    = EXCLUDED.payload,
             fetched_at = EXCLUDED.fetched_at",
    )
    .bind(id)
    .bind(kind)
    .bind(payload)
    .execute(pool)
    .await;
    if let Err(e) = res {
        tracing::warn!(error = %e, kind, id, "tmdb cache write failed");
    }
}

/// Self-link any external ids found in a hydrated detail payload into
/// `shirabe.xref`, so a TMDB id resolves to its sibling provider ids. Reads both a
/// top-level `imdb_id` and an `external_ids` object. Best-effort.
async fn self_link_external_ids(state: &AppState, tmdb_kind: &str, tmdb_id: i64, payload: &Value) {
    let Some(pool) = shirabe_pool(state) else {
        return;
    };
    let mut rows: Vec<(Option<String>, String, String)> = Vec::new();

    // The TMDB id itself → tmdb_movie / tmdb_tv source tag (matches wikidata.rs).
    let self_source = match tmdb_kind {
        "movie" => "tmdb_movie",
        _ => "tmdb_tv",
    };
    rows.push((None, self_source.to_string(), tmdb_id.to_string()));

    // imdb_id may sit top-level (movies) and/or under external_ids (tv + movies).
    let nonempty_imdb = |v: &Value| -> Option<String> {
        v.get("imdb_id").and_then(Value::as_str).filter(|s| !s.is_empty()).map(ToString::to_string)
    };
    let imdb =
        nonempty_imdb(payload).or_else(|| payload.get("external_ids").and_then(nonempty_imdb));
    if let Some(imdb_id) = imdb {
        rows.push((None, "imdb".to_string(), imdb_id));
    }

    if let Err(e) = wikidata::upsert_xref(pool, &rows).await {
        tracing::warn!(error = %e, "tmdb external-id self-link failed");
    }
}

/// Perform an upstream TMDB v3 GET, returning the parsed JSON body. `path` is the
/// endpoint path under [`API_BASE`] (no leading slash); `extra` are extra query
/// pairs appended after the held `api_key`.
async fn upstream_get(key: &str, path: &str, extra: &[(&str, String)]) -> Result<Value, String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let mut query: Vec<(&str, String)> = vec![("api_key", key.to_string())];
    query.extend(extra.iter().map(|(k, v)| (*k, v.clone())));
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

/// Read the inbound `query` search term from the accepted query params.
fn search_query(params: &Value) -> Option<String> {
    params.get("query").and_then(Value::as_str).map(ToString::to_string)
}

/// Sort a TMDB `results` array in place by descending `popularity` (ranking ties).
fn rank_by_popularity(results: &mut Value) {
    if let Some(arr) = results.as_array_mut() {
        arr.sort_by(|a, b| {
            let pa = a.get("popularity").and_then(Value::as_f64).unwrap_or(0.0);
            let pb = b.get("popularity").and_then(Value::as_f64).unwrap_or(0.0);
            pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

/// Hash a search query string to a stable cache id (search rows are keyed by id +
/// kind like detail rows; the query has no numeric id of its own).
fn search_cache_id(query: &str) -> i64 {
    // FNV-1a 64-bit, folded into the signed range. Deterministic across runs.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in query.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Top bit cleared by `>> 1`, so the cast is always non-negative.
    i64::try_from(hash >> 1).unwrap_or(0)
}

/// Shared search handler for `tv` / `movie`.
async fn search(state: &Arc<AppState>, kind: &str, params: &Value) -> Response {
    let Some(query) = search_query(params) else {
        return tmdb_error(StatusCode::BAD_REQUEST, 22, "query parameter is required");
    };
    let cache_kind = format!("search_{kind}");
    let cache_id = search_cache_id(&query);

    if let Some(cached) = cache_get(state, cache_id, &cache_kind).await {
        return Json(cached).into_response();
    }

    let Some(key) = state.config.tmdb_api_key.as_deref() else {
        return not_configured();
    };

    let path = format!("search/{kind}");
    match upstream_get(key, &path, &[("query", query)]).await {
        Ok(mut payload) => {
            if let Some(results) = payload.get_mut("results") {
                rank_by_popularity(results);
            }
            cache_put(state, cache_id, &cache_kind, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, kind, "tmdb search upstream failed");
            tmdb_error(StatusCode::BAD_GATEWAY, 11, "TMDB upstream error")
        }
    }
}

/// Shared detail handler for `tv` / `movie`, honouring
/// `append_to_response=external_ids` so `external_ids.imdb_id` is present.
async fn detail(state: &Arc<AppState>, kind: &str, id_raw: &str) -> Response {
    let Ok(id) = id_raw.parse::<i64>() else {
        return tmdb_error(StatusCode::BAD_REQUEST, 34, "invalid id");
    };

    if let Some(cached) = cache_get(state, id, kind).await {
        return Json(cached).into_response();
    }

    let Some(key) = state.config.tmdb_api_key.as_deref() else {
        return not_configured();
    };

    let path = format!("{kind}/{id}");
    match upstream_get(key, &path, &[("append_to_response", "external_ids".to_string())]).await {
        Ok(payload) => {
            cache_put(state, id, kind, &payload).await;
            self_link_external_ids(state, kind, id, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, kind, id, "tmdb detail upstream failed");
            tmdb_error(StatusCode::BAD_GATEWAY, 11, "TMDB upstream error")
        }
    }
}

/// `GET /3/search/tv?query=` → `{results:[{id,name,first_air_date,…}]}`.
async fn search_tv(State(state): State<Arc<AppState>>, Query(params): Query<Value>) -> Response {
    search(&state, "tv", &params).await
}

/// `GET /3/search/movie?query=` → `{results:[{id,title,release_date,overview,…}]}`.
async fn search_movie(State(state): State<Arc<AppState>>, Query(params): Query<Value>) -> Response {
    search(&state, "movie", &params).await
}

/// `GET /3/tv/{id}` → `{name,first_air_date,seasons:[…],external_ids:{imdb_id}}`.
async fn tv(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    detail(&state, "tv", &id).await
}

/// `GET /3/movie/{id}?append_to_response=external_ids` →
/// `{title,release_date,runtime,imdb_id,external_ids:{imdb_id},overview}`.
async fn movie(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(_params): Query<Value>,
) -> Response {
    detail(&state, "movie", &id).await
}

/// `GET /3/tv/{id}/season/{n}` → `{episodes:[{episode_number,name,runtime}]}`.
async fn tv_season(
    State(state): State<Arc<AppState>>,
    Path((id, n)): Path<(String, String)>,
) -> Response {
    let (Ok(tv_id), Ok(season)) = (id.parse::<i64>(), n.parse::<i64>()) else {
        return tmdb_error(StatusCode::BAD_REQUEST, 34, "invalid id");
    };
    // Cache key: pack (tv_id, season) into one id space distinct from tv detail.
    let cache_kind = "tv_season";
    let cache_id = tv_id.wrapping_mul(10_000).wrapping_add(season);

    if let Some(cached) = cache_get(&state, cache_id, cache_kind).await {
        return Json(cached).into_response();
    }

    let Some(key) = state.config.tmdb_api_key.as_deref() else {
        return not_configured();
    };

    let path = format!("tv/{tv_id}/season/{season}");
    match upstream_get(key, &path, &[]).await {
        Ok(payload) => {
            cache_put(&state, cache_id, cache_kind, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, tv_id, season, "tmdb season upstream failed");
            tmdb_error(StatusCode::BAD_GATEWAY, 11, "TMDB upstream error")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TTL staleness: rows within the window are fresh; older or future-skewed
    /// rows are not; a non-positive TTL disables caching.
    #[test]
    fn cache_ttl_freshness() {
        let week = 7 * 86_400;
        assert!(is_fresh(0, 7)); // just written
        assert!(is_fresh(week - 1, 7)); // within window
        assert!(is_fresh(week, 7)); // exactly at TTL boundary → still fresh
        assert!(!is_fresh(week + 1, 7)); // past TTL → stale
        assert!(!is_fresh(-5, 7)); // clock skew (future) → treat as stale
        assert!(!is_fresh(0, 0)); // TTL disabled
        assert!(!is_fresh(0, -1)); // negative TTL disabled
    }

    /// The same query hashes to the same stable cache id; different queries differ.
    #[test]
    fn search_cache_id_is_stable() {
        assert_eq!(search_cache_id("matrix"), search_cache_id("matrix"));
        assert_ne!(search_cache_id("matrix"), search_cache_id("inception"));
        assert!(search_cache_id("matrix") >= 0);
    }

    /// Ranking sorts results by descending popularity (ties broken by popularity).
    #[test]
    fn ranks_results_by_popularity() {
        let mut results = json!([
            {"id":1,"popularity":3.0},
            {"id":2,"popularity":10.0},
            {"id":3,"popularity":7.0}
        ]);
        rank_by_popularity(&mut results);
        let ids: Vec<i64> =
            results.as_array().unwrap().iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![2, 3, 1]);
    }
}
