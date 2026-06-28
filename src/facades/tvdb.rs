//! TheTVDB v4 facade (SHIB-7).
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ ATTRIBUTION / LICENSING — TheTVDB                                           │
//! │                                                                            │
//! │ Metadata provided by TheTVDB (https://thetvdb.com). This deployment uses a │
//! │ single licensed project API key (+ optional operator PIN) held strictly    │
//! │ server-side. The real key/PIN are NEVER re-exposed to clients: `/v4/login` │
//! │ ACCEPTS any client apikey/pin and mints a Shirabe token instead, while the │
//! │ real key is used in-process to obtain the upstream bearer.                 │
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! Mounts the exact `/v4/*` endpoints Kusaritoi's TVDB provider calls, mirroring
//! the upstream v4 JSON shapes. Each data handler is cache-first: it serves a fresh
//! row from `shirabe.tvdb_cache` (TTL = `TVDB_CACHE_TTL_DAYS`, default 7d) when
//! present, otherwise calls the v4 API once with the in-memory server bearer, stores
//! the payload, and self-links any returned `remoteIds` into `shirabe.xref` via
//! [`wikidata::upsert_xref`]. A second identical call is served from cache and never
//! hits upstream.
//!
//! `name`, `aliases`, and `translations` are preserved verbatim in the cached
//! payload so non-latin names survive (Kusaritoi scores against these).
//!
//! Graceful degradation: when `TVDB_API_KEY` is unset, a request that would need
//! upstream returns a clean failure in TheTVDB's `{status:"failure", message}` shape
//! (HTTP 503) — never a panic — while cached rows are still served. The API server
//! still boots and serves `/ws/2` + the other facades.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::AppState;
use crate::sources::tvdb::API_BASE;
use crate::sources::wikidata;

/// Build the `/v4` route group.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v4/login", post(login))
        .route("/v4/search", get(search))
        .route("/v4/series/{id}", get(series))
        .route("/v4/series/{id}/extended", get(series_extended))
        .route("/v4/series/{id}/episodes/{season_type}", get(series_episodes))
        .route("/v4/movies/{id}", get(movie))
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

/// TheTVDB-shaped failure body + status. Used when the key is absent or upstream
/// fails. Shape: `{ "status": "failure", "message": "…" }`.
fn tvdb_failure(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "status": "failure", "message": message }))).into_response()
}

/// The 503 returned when no server-side key is configured and the request can't be
/// served from cache.
fn not_configured() -> Response {
    tvdb_failure(StatusCode::SERVICE_UNAVAILABLE, "TheTVDB source not configured")
}

/// The writable `shirabe` pool, or `None` when `SHIRABE_DATABASE_URL` is unset.
const fn shirabe_pool(state: &AppState) -> Option<&PgPool> {
    state.pools.shirabe.as_ref()
}

/// Fetch a fresh cached payload for `(id, kind)`, honouring the configured TTL.
/// The freshness test is done in SQL (`fetched_at` vs `now()`). Returns `None` on
/// miss / stale / no pool.
async fn cache_get(state: &AppState, id: i64, kind: &str) -> Option<Value> {
    let pool = shirabe_pool(state)?;
    let ttl_days = state.config.tvdb_cache_ttl_days;
    if ttl_days <= 0 {
        return None;
    }
    let row = sqlx::query(
        "SELECT payload FROM shirabe.tvdb_cache
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

/// Store (upsert) a payload into `shirabe.tvdb_cache` with `fetched_at = now()`.
/// Best-effort: a cache write failure is logged but does not fail the request.
async fn cache_put(state: &AppState, id: i64, kind: &str, payload: &Value) {
    let Some(pool) = shirabe_pool(state) else {
        return;
    };
    let res = sqlx::query(
        "INSERT INTO shirabe.tvdb_cache (id, kind, payload, fetched_at)
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
        tracing::warn!(error = %e, kind, id, "tvdb cache write failed");
    }
}

/// Hash a search/string key to a stable non-negative cache id (search rows and
/// movie/series ids that aren't plain integers share the (id, kind) cache space).
fn stable_cache_id(key: &str) -> i64 {
    // FNV-1a 64-bit, folded into the signed range. Deterministic across runs.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::try_from(hash >> 1).unwrap_or(0)
}

/// Parse a TheTVDB id that may be either a bare integer (`1396`) or a prefixed slug
/// (`series-1396` / `movie-1396`) into the numeric id used upstream.
fn numeric_id(raw: &str) -> Option<i64> {
    raw.rsplit('-').next().unwrap_or(raw).parse::<i64>().ok()
}

/// Extract `remoteIds` from a hydrated v4 payload and self-link them into
/// `shirabe.xref`, so a TVDB id resolves to its sibling provider ids. Best-effort.
///
/// A v4 `remoteIds` entry looks like `{ "id": "tt0903747", "type": 2,
/// "sourceName": "IMDB" }`. We map known `sourceName`s to xref source tags and
/// always record the TVDB id itself.
async fn self_link_remote_ids(state: &AppState, tvdb_id: i64, payload: &Value) {
    let Some(pool) = shirabe_pool(state) else {
        return;
    };
    let mut rows: Vec<(Option<String>, String, String)> = Vec::new();
    // The TVDB id itself → `tvdb` source tag (matches wikidata.rs).
    rows.push((None, "tvdb".to_string(), tvdb_id.to_string()));

    // `remoteIds` may sit at the payload top level or under `data`.
    let remote_ids = payload
        .get("remoteIds")
        .or_else(|| payload.get("data").and_then(|d| d.get("remoteIds")))
        .and_then(Value::as_array);
    if let Some(entries) = remote_ids {
        for entry in entries {
            let Some(id) = entry.get("id").and_then(Value::as_str).filter(|s| !s.is_empty()) else {
                continue;
            };
            let source =
                entry.get("sourceName").and_then(Value::as_str).map(str::to_ascii_lowercase);
            let tag = match source.as_deref() {
                Some(s) if s.contains("imdb") => Some("imdb"),
                Some(s) if s.contains("themoviedb") || s.contains("tmdb") => {
                    // TheTVDB does not disambiguate movie vs tv here; default to tv.
                    Some("tmdb_tv")
                }
                _ => None,
            };
            if let Some(tag) = tag {
                rows.push((None, tag.to_string(), id.to_string()));
            }
        }
    }

    if let Err(e) = wikidata::upsert_xref(pool, &rows).await {
        tracing::warn!(error = %e, "tvdb remote-id self-link failed");
    }
}

/// Perform an upstream TheTVDB v4 GET with the in-memory server bearer, returning
/// the parsed JSON body. `path` is the endpoint path under [`API_BASE`] (no leading
/// slash); `extra` are query pairs. Refreshes the token once on a 401 and retries.
async fn upstream_get(
    state: &Arc<AppState>,
    path: &str,
    extra: &[(&str, String)],
) -> Result<Value, String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let url = format!("{API_BASE}/{path}");

    let mut bearer = state.tvdb_tokens.bearer(&state.config).await?;
    for attempt in 0..2 {
        let resp = client
            .get(&url)
            .bearer_auth(&bearer)
            .query(extra)
            .send()
            .await
            .map_err(|e| format!("upstream request: {e}"))?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
            // Token lapsed early; force a fresh login and retry once.
            bearer = state.tvdb_tokens.force_refresh(&state.config).await?;
            continue;
        }
        let resp = resp.error_for_status().map_err(|e| format!("upstream status: {e}"))?;
        let bytes = resp.bytes().await.map_err(|e| format!("upstream body: {e}"))?;
        return serde_json::from_slice::<Value>(&bytes).map_err(|e| format!("upstream json: {e}"));
    }
    Err("upstream auth retry exhausted".to_string())
}

/// `POST /v4/login` — accept any client apikey/pin and mint a Shirabe token.
///
/// The real project key/PIN stay server-side; we do NOT proxy the client's creds.
/// For the single-tenant homelab the minted token is a constant the facade also
/// accepts back as a Bearer (any Bearer is accepted on data calls). When no
/// server-side key is configured this still returns a clean failure (so Kusaritoi
/// sees a graceful error), never a 500.
async fn login(State(state): State<Arc<AppState>>) -> Response {
    if state.config.tvdb_api_key.is_none() {
        return not_configured();
    }
    // Mint a Shirabe-scoped token. It is opaque to the client and never carries the
    // real key. The data handlers accept any Bearer in single-tenant operation.
    Json(json!({ "data": { "token": SHIRABE_TOKEN } })).into_response()
}

/// The opaque token `/v4/login` mints. Constant for the single-tenant homelab; it
/// never embeds the real upstream key/PIN.
const SHIRABE_TOKEN: &str = "shirabe-tvdb-token";

/// `GET /v4/search?type=series&query=` → `{data:[{tvdb_id,name,year,aliases,
/// translations,…}]}`. Cache-first; `name`/`aliases`/`translations` preserved.
async fn search(State(state): State<Arc<AppState>>, Query(params): Query<Value>) -> Response {
    let Some(query) = params.get("query").and_then(Value::as_str) else {
        return tvdb_failure(StatusCode::BAD_REQUEST, "query parameter is required");
    };
    let search_type = params.get("type").and_then(Value::as_str).unwrap_or("series").to_string();
    let cache_kind = format!("search_{search_type}");
    let cache_id = stable_cache_id(query);

    if let Some(cached) = cache_get(&state, cache_id, &cache_kind).await {
        return Json(cached).into_response();
    }
    if state.config.tvdb_api_key.is_none() {
        return not_configured();
    }

    let extra = [("type", search_type), ("query", query.to_string())];
    match upstream_get(&state, "search", &extra).await {
        Ok(payload) => {
            cache_put(&state, cache_id, &cache_kind, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "tvdb search upstream failed");
            tvdb_failure(StatusCode::BAD_GATEWAY, "TheTVDB upstream error")
        }
    }
}

/// Shared series detail handler for `/v4/series/{id}` and `/v4/series/{id}/extended`.
/// `extended` rides the `…/extended` upstream path and carries `remoteIds`.
async fn series_detail(state: &Arc<AppState>, id_raw: &str, extended: bool) -> Response {
    let Some(id) = numeric_id(id_raw) else {
        return tvdb_failure(StatusCode::BAD_REQUEST, "invalid series id");
    };
    let cache_kind = if extended { "series_extended" } else { "series" };

    if let Some(cached) = cache_get(state, id, cache_kind).await {
        return Json(cached).into_response();
    }
    if state.config.tvdb_api_key.is_none() {
        return not_configured();
    }

    let path = if extended { format!("series/{id}/extended") } else { format!("series/{id}") };
    match upstream_get(state, &path, &[]).await {
        Ok(payload) => {
            cache_put(state, id, cache_kind, &payload).await;
            self_link_remote_ids(state, id, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, id, extended, "tvdb series upstream failed");
            tvdb_failure(StatusCode::BAD_GATEWAY, "TheTVDB upstream error")
        }
    }
}

/// `GET /v4/series/{id}`.
async fn series(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    series_detail(&state, &id, false).await
}

/// `GET /v4/series/{id}/extended` → `{data:{name,firstAired,seasons:[…],remoteIds}}`.
async fn series_extended(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    series_detail(&state, &id, true).await
}

/// `GET /v4/series/{id}/episodes/{season_type}?season=&page=` → paginated episodes.
/// `{data:{episodes:[…]}, links:{next}}`. One page is fetched per call (Kusaritoi
/// walks `links.next`); the cache key includes the season type, season, and page.
async fn series_episodes(
    State(state): State<Arc<AppState>>,
    Path((id_raw, season_type)): Path<(String, String)>,
    Query(params): Query<Value>,
) -> Response {
    let Some(id) = numeric_id(&id_raw) else {
        return tvdb_failure(StatusCode::BAD_REQUEST, "invalid series id");
    };
    let season = params.get("season").and_then(value_as_str_or_num);
    let page = params.get("page").and_then(value_as_str_or_num).unwrap_or_else(|| "0".to_string());

    let cache_key = format!("{id}:{season_type}:{}:{page}", season.clone().unwrap_or_default());
    let cache_kind = "series_episodes";
    let cache_id = stable_cache_id(&cache_key);

    if let Some(cached) = cache_get(&state, cache_id, cache_kind).await {
        return Json(cached).into_response();
    }
    if state.config.tvdb_api_key.is_none() {
        return not_configured();
    }

    let mut extra: Vec<(&str, String)> = vec![("page", page)];
    if let Some(season) = season {
        extra.push(("season", season));
    }
    let path = format!("series/{id}/episodes/{season_type}");
    match upstream_get(&state, &path, &extra).await {
        Ok(payload) => {
            cache_put(&state, cache_id, cache_kind, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, id, "tvdb episodes upstream failed");
            tvdb_failure(StatusCode::BAD_GATEWAY, "TheTVDB upstream error")
        }
    }
}

/// Read a query param that may arrive as a JSON string or number into a string.
fn value_as_str_or_num(v: &Value) -> Option<String> {
    v.as_str().map(ToString::to_string).or_else(|| v.as_i64().map(|n| n.to_string()))
}

/// `GET /v4/movies/{id}` → movie record (cross-IDs via `remoteIds`).
async fn movie(State(state): State<Arc<AppState>>, Path(id_raw): Path<String>) -> Response {
    let Some(id) = numeric_id(&id_raw) else {
        return tvdb_failure(StatusCode::BAD_REQUEST, "invalid movie id");
    };
    let cache_kind = "movie";

    if let Some(cached) = cache_get(&state, id, cache_kind).await {
        return Json(cached).into_response();
    }
    if state.config.tvdb_api_key.is_none() {
        return not_configured();
    }

    // The extended movie record carries remoteIds for cross-linking.
    let path = format!("movies/{id}/extended");
    match upstream_get(&state, &path, &[]).await {
        Ok(payload) => {
            cache_put(&state, id, cache_kind, &payload).await;
            self_link_remote_ids(&state, id, &payload).await;
            Json(payload).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, id, "tvdb movie upstream failed");
            tvdb_failure(StatusCode::BAD_GATEWAY, "TheTVDB upstream error")
        }
    }
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

    /// A bare integer, a `series-` slug, and a `movie-` slug all resolve to the
    /// numeric id; non-numeric tails are rejected.
    #[test]
    fn parses_numeric_and_slug_ids() {
        assert_eq!(numeric_id("1396"), Some(1396));
        assert_eq!(numeric_id("series-1396"), Some(1396));
        assert_eq!(numeric_id("movie-42"), Some(42));
        assert_eq!(numeric_id("series-abc"), None);
        assert_eq!(numeric_id(""), None);
    }

    /// Stable cache id is deterministic, query-sensitive, and non-negative.
    #[test]
    fn stable_cache_id_is_stable() {
        assert_eq!(stable_cache_id("breaking bad"), stable_cache_id("breaking bad"));
        assert_ne!(stable_cache_id("breaking bad"), stable_cache_id("better call saul"));
        assert!(stable_cache_id("breaking bad") >= 0);
    }

    /// A v4 search payload carrying a non-latin `name`, `aliases`, and
    /// `translations` round-trips through serde untouched — Kusaritoi scores against
    /// these, so they must survive verbatim in the cached payload.
    #[test]
    fn search_payload_preserves_aliases_and_translations() {
        let raw = r#"{ "data": [ {
            "tvdb_id": "series-81797",
            "name": "ワンピース",
            "year": "1999",
            "aliases": ["One Piece", "ワンピース"],
            "translations": { "jpn": "ワンピース", "eng": "One Piece" }
        } ] }"#;
        let parsed: Value = serde_json::from_str(raw).expect("parses");
        let first = &parsed["data"][0];
        assert_eq!(first["name"], "ワンピース");
        assert_eq!(first["aliases"][1], "ワンピース");
        assert_eq!(first["translations"]["jpn"], "ワンピース");
        // Re-serialising preserves the non-latin glyphs (no ascii-escaping loss).
        let round = serde_json::to_string(&parsed).expect("serialises");
        assert!(round.contains("ワンピース"));
    }

    /// A v4 series-extended record exposes its `remoteIds`; the IMDb id is picked up
    /// for self-linking and a bare integer id is used.
    #[test]
    fn series_extended_remote_ids_extractable() {
        let raw = r#"{ "data": {
            "id": 81797,
            "name": "One Piece",
            "remoteIds": [
                { "id": "tt0388629", "type": 2, "sourceName": "IMDB" },
                { "id": "37854", "type": 12, "sourceName": "TheMovieDB.com" }
            ]
        } }"#;
        let parsed: Value = serde_json::from_str(raw).expect("parses");
        let remote = parsed["data"]["remoteIds"].as_array().expect("array");
        let imdb = remote
            .iter()
            .find(|e| e["sourceName"].as_str() == Some("IMDB"))
            .and_then(|e| e["id"].as_str());
        assert_eq!(imdb, Some("tt0388629"));
    }
}
